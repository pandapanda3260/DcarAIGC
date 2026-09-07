"""Schema 18 contract and offline-only account lineage migration."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from typing import Any

MIGRATION_NAME = "matrix-roster-source-routing"
NEW_TABLES = (
    "account_roster_snapshots",
    "account_roster_members",
    "account_metric_observations",
)

MATRIX_SCHEMA_SQL = r"""
CREATE INDEX IF NOT EXISTS idx_accounts_phone_normalized ON accounts(phone_normalized);
CREATE UNIQUE INDEX IF NOT EXISTS uq_account_provider_reference_value
ON account_provider_references(provider, reference_kind, reference_value);
CREATE UNIQUE INDEX IF NOT EXISTS uq_metric_snapshot_canonical
ON content_metric_snapshots(content_id, window_key);

CREATE TABLE IF NOT EXISTS account_roster_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL
        CHECK(source_type IN ('bootstrap_export','manual_export','api_fullroster')),
    scope_key TEXT NOT NULL,
    scope_json TEXT NOT NULL CHECK(json_valid(scope_json)),
    source_instance_id TEXT NOT NULL,
    source_captured_at TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    declared_count INTEGER NOT NULL CHECK(declared_count>=0),
    member_count INTEGER NOT NULL CHECK(member_count=declared_count),
    members_sha256 TEXT NOT NULL CHECK(length(members_sha256)=64),
    source_sha256 TEXT NOT NULL CHECK(length(source_sha256)=64),
    source_path TEXT NOT NULL,
    contract_version TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(scope_key, source_instance_id)
);

CREATE INDEX IF NOT EXISTS idx_roster_snapshots_current
ON account_roster_snapshots(scope_key, accepted_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS account_roster_members (
    snapshot_id INTEGER NOT NULL REFERENCES account_roster_snapshots(id) ON DELETE RESTRICT,
    account_identity_id INTEGER NOT NULL REFERENCES account_platform_identities(id) ON DELETE RESTRICT,
    platform TEXT NOT NULL CHECK(platform IN ('douyin','xiaohongshu','wechat_channels','kuaishou')),
    matrix_account_id TEXT NOT NULL CHECK(length(trim(matrix_account_id))>0),
    profile_ref TEXT NOT NULL CHECK(length(trim(profile_ref))>0),
    monitoring_status TEXT NOT NULL CHECK(monitoring_status IN ('unknown','monitored','not_monitored')),
    authorization_status TEXT NOT NULL CHECK(authorization_status IN ('unknown','authorized','unauthorized')),
    monitoring_started_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    PRIMARY KEY(snapshot_id, account_identity_id),
    UNIQUE(snapshot_id, platform, matrix_account_id)
);

CREATE INDEX IF NOT EXISTS idx_roster_members_identity
ON account_roster_members(account_identity_id, snapshot_id);

CREATE TABLE IF NOT EXISTS account_metric_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_identity_id INTEGER NOT NULL REFERENCES account_platform_identities(id) ON DELETE RESTRICT,
    source TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    raw_response_id INTEGER REFERENCES provider_raw_responses(id) ON DELETE RESTRICT,
    contract_version TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
    observation_sha256 TEXT NOT NULL UNIQUE CHECK(length(observation_sha256)=64)
);

CREATE INDEX IF NOT EXISTS idx_account_metrics_identity_capture
ON account_metric_observations(account_identity_id, captured_at DESC, recorded_at DESC, id DESC);

CREATE TRIGGER IF NOT EXISTS trg_roster_snapshots_no_update
BEFORE UPDATE ON account_roster_snapshots
BEGIN
    SELECT RAISE(ABORT, 'accepted roster snapshots are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_roster_snapshots_no_delete
BEFORE DELETE ON account_roster_snapshots
BEGIN
    SELECT RAISE(ABORT, 'accepted roster snapshots are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_roster_members_no_update
BEFORE UPDATE ON account_roster_members
BEGIN
    SELECT RAISE(ABORT, 'accepted roster members are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_roster_members_no_delete
BEFORE DELETE ON account_roster_members
BEGIN
    SELECT RAISE(ABORT, 'accepted roster members are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_roster_members_identity_platform
BEFORE INSERT ON account_roster_members
WHEN NOT EXISTS (
    SELECT 1 FROM account_platform_identities
    WHERE id=NEW.account_identity_id AND platform=NEW.platform
)
BEGIN
    SELECT RAISE(ABORT, 'roster member platform differs from identity');
END;

CREATE TRIGGER IF NOT EXISTS trg_roster_members_declared_count
BEFORE INSERT ON account_roster_members
WHEN (
    SELECT COUNT(*) FROM account_roster_members WHERE snapshot_id=NEW.snapshot_id
) >= (
    SELECT member_count FROM account_roster_snapshots WHERE id=NEW.snapshot_id
)
BEGIN
    SELECT RAISE(ABORT, 'accepted roster exceeds declared member count');
END;

CREATE TRIGGER IF NOT EXISTS trg_account_metrics_no_update
BEFORE UPDATE ON account_metric_observations
BEGIN
    SELECT RAISE(ABORT, 'account metric observations are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_account_metrics_no_delete
BEFORE DELETE ON account_metric_observations
BEGIN
    SELECT RAISE(ABORT, 'account metric observations are immutable');
END;
"""

ACCOUNT_SQL = """CREATE TABLE accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phone TEXT NOT NULL,
    phone_normalized TEXT,
    operator_name TEXT NOT NULL DEFAULT '',
    account_type TEXT NOT NULL DEFAULT 'unknown'
        CHECK(account_type IN ('boutique_ip','original','mixed_edit','unknown')),
    content_direction TEXT NOT NULL DEFAULT 'unknown'
        CHECK(content_direction IN ('new_car','used_car','media','other','unknown')),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)"""

IDENTITY_SQL = """CREATE TABLE account_platform_identities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    platform TEXT NOT NULL CHECK(platform IN ('douyin','xiaohongshu','wechat_channels','kuaishou')),
    uid TEXT,
    nickname TEXT NOT NULL DEFAULT '',
    real_name_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK(real_name_status IN ('yes','no','unknown')),
    source TEXT NOT NULL DEFAULT 'manual',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(platform, uid),
    UNIQUE(account_id)
)"""


def _statements(sql: str) -> list[str]:
    statements: list[str] = []
    pending = ""
    for line in sql.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statements.append(pending.strip().removesuffix(";").strip())
            pending = ""
    if pending.strip():
        raise RuntimeError("incomplete schema18 SQL statement")
    return statements


def expected_schema_objects() -> dict[tuple[str, str], str]:
    from . import storage

    expected = {
        (kind, name): sql for kind, name, sql in storage._frozen_schema_objects(17)
    }
    expected[("table", "accounts")] = ACCOUNT_SQL
    expected[("table", "account_platform_identities")] = IDENTITY_SQL
    for statement in _statements(MATRIX_SCHEMA_SQL):
        match = re.match(
            r"CREATE (?:UNIQUE )?(TABLE|INDEX|TRIGGER) IF NOT EXISTS ([a-z_]+)\b",
            statement,
        )
        if match is None:
            raise RuntimeError("invalid schema18 object declaration")
        expected[(match[1].lower(), match[2])] = statement.replace(
            " IF NOT EXISTS", "", 1
        )
    return expected


def validate_structure(connection: sqlite3.Connection) -> None:
    from . import storage

    expected = expected_schema_objects()
    actual = {
        (str(row[0]), str(row[1])): str(row[2])
        for row in connection.execute(
            "SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'"
        )
    }
    if set(actual) != set(expected):
        raise storage.SchemaMigrationError(
            "schema v18 object set differs from sealed contract"
        )
    for key, definition in expected.items():
        allowed = {storage._normalized_historical_sql(definition)}
        if key[0] == "table" and key[1] in storage._historical_table_variants():
            allowed.add(
                storage._normalized_historical_sql(
                    storage._historical_table_variants()[key[1]]
                )
            )
        if storage._normalized_historical_sql(actual[key]) not in allowed:
            raise storage.SchemaMigrationError(
                f"schema v18 object definition drifted: {key[1]}"
            )


_ADAPTER_PLATFORMS = {
    "tikhub-reference-validate-v1": "douyin",
    "tikhub-uid-encrypt-v1": "douyin",
    "tikhub-uid-profile-v8.1": "douyin",
    "tikhub-uid-reference-v8.0": "douyin",
    "tikhub-user-posts-v8.1": "douyin",
    "tikhub-xhs-app-v2-user-posts-v8.1": "xiaohongshu",
}
_OPERATION_PLATFORMS = {
    "douyin_reference_validation_posts": "douyin",
    "douyin_uid_profile": "douyin",
    "douyin_uid_to_sec_user_id": "douyin",
    "douyin_user_posts": "douyin",
    "douyin_video_detail": "douyin",
    "douyin_comments": "douyin",
    "xiaohongshu_user_posts": "xiaohongshu",
    "xiaohongshu_note_detail": "xiaohongshu",
    "xiaohongshu_video_detail": "xiaohongshu",
    "xiaohongshu_comments": "xiaohongshu",
}


def _fail(message: str) -> None:
    from .storage import SchemaMigrationError

    raise SchemaMigrationError(message)


def _single_platform(signals: set[str], label: str) -> str:
    if len(signals) != 1:
        _fail(f"v18 cannot prove platform for {label}: {sorted(signals)}")
    return next(iter(signals))


def _sequences(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        "SELECT name,seq FROM sqlite_sequence ORDER BY name"
    ).fetchall()
    result = {str(row[0]): int(row[1]) for row in rows}
    if len(result) != len(rows):
        _fail("v18 sqlite_sequence has duplicate names")
    return result


def migration_plan(connection: sqlite3.Connection) -> dict[str, Any]:
    """Recompute exact ID changes from a genuine v17 source; do not write it."""
    from . import storage

    storage.require_schema_compatibility(connection, supported_versions=frozenset({17}))
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        _fail("v18 source has foreign-key violations")
    if connection.execute(
        "SELECT 1 FROM content_metric_snapshots GROUP BY content_id,window_key HAVING COUNT(*)>1 LIMIT 1"
    ).fetchone():
        _fail("v18 canonical snapshot conflict; no implicit merge is allowed")
    if connection.execute(
        "SELECT 1 FROM account_provider_references "
        "GROUP BY provider,reference_kind,reference_value HAVING COUNT(*)>1 LIMIT 1"
    ).fetchone():
        _fail("v18 provider reference collision")
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in connection.execute(
        "SELECT * FROM account_platform_identities ORDER BY account_id,id"
    ):
        grouped.setdefault(int(row["account_id"]), []).append(dict(row))
    max_account_id = int(
        connection.execute("SELECT COALESCE(MAX(id),0) FROM accounts").fetchone()[0]
    )
    sequences = _sequences(connection)
    next_id = max(max_account_id, sequences.get("accounts", 0))
    splits: list[dict[str, Any]] = []
    historical_mixed_slots: list[dict[str, Any]] = []
    updates: dict[str, list[dict[str, Any]]] = {
        table: []
        for table in (
            "account_platform_identities",
            "content_items",
            "fetch_slots",
            "provider_raw_responses",
        )
    }
    for old_id, identities in sorted(grouped.items()):
        if len(identities) < 2:
            continue
        by_platform = {str(item["platform"]): item for item in identities}
        if len(identities) != 2 or set(by_platform) != {"douyin", "xiaohongshu"}:
            _fail(f"v18 unsupported multi-platform account {old_id}")
        next_id += 1
        new_id = next_id
        moved = by_platform["xiaohongshu"]
        splits.append(
            {
                "old_account_id": old_id,
                "new_account_id": new_id,
                "retained_identity_id": int(by_platform["douyin"]["id"]),
                "moved_identity_id": int(moved["id"]),
                "retained_platform": "douyin",
                "moved_platform": "xiaohongshu",
            }
        )
        updates["account_platform_identities"].append(
            {
                "id": int(moved["id"]),
                "old_account_id": old_id,
                "new_account_id": new_id,
            }
        )
        for row in connection.execute(
            "SELECT id FROM content_items WHERE account_id=? AND platform='xiaohongshu' ORDER BY id",
            (old_id,),
        ):
            updates["content_items"].append(
                {
                    "id": int(row[0]),
                    "old_account_id": old_id,
                    "new_account_id": new_id,
                }
            )
        slot_platforms: dict[int, str] = {}
        for slot in connection.execute(
            "SELECT id,adapter_version FROM fetch_slots WHERE account_id=? ORDER BY id",
            (old_id,),
        ):
            signals: set[str] = set()
            adapter_platform = _ADAPTER_PLATFORMS.get(str(slot["adapter_version"]))
            raw_origins: list[dict[str, Any]] = []
            for raw in connection.execute(
                "SELECT r.id,r.operation,c.platform FROM provider_raw_responses r "
                "JOIN fetch_attempts a ON a.id=r.fetch_attempt_id "
                "LEFT JOIN content_items c ON c.id=r.content_id WHERE a.slot_id=?",
                (slot["id"],),
            ):
                platform = _OPERATION_PLATFORMS.get(str(raw["operation"]))
                if platform:
                    signals.add(platform)
                    raw_origins.append(
                        {
                            "raw_response_id": int(raw["id"]),
                            "operation": str(raw["operation"]),
                            "platform": platform,
                        }
                    )
                if raw["platform"]:
                    signals.add(str(raw["platform"]))
            platform = adapter_platform or _single_platform(
                signals, f"fetch_slot:{slot['id']}"
            )
            if adapter_platform and signals - {adapter_platform}:
                historical_mixed_slots.append(
                    {
                        "slot_id": int(slot["id"]),
                        "old_account_id": old_id,
                        "adapter_version": str(slot["adapter_version"]),
                        "slot_platform": adapter_platform,
                        "raw_origins": raw_origins,
                        "policy": "slot_by_adapter_raw_by_operation_preserve_attempt_links",
                    }
                )
            slot_platforms[int(slot["id"])] = platform
            if platform == "xiaohongshu":
                updates["fetch_slots"].append(
                    {
                        "id": int(slot["id"]),
                        "old_account_id": old_id,
                        "new_account_id": new_id,
                    }
                )
        for raw in connection.execute(
            "SELECT r.id,r.operation,c.platform,a.slot_id,s.account_id AS slot_account_id "
            "FROM provider_raw_responses r "
            "LEFT JOIN content_items c ON c.id=r.content_id "
            "LEFT JOIN fetch_attempts a ON a.id=r.fetch_attempt_id "
            "LEFT JOIN fetch_slots s ON s.id=a.slot_id "
            "WHERE r.account_id=? ORDER BY r.id",
            (old_id,),
        ):
            signals = set()
            platform = _OPERATION_PLATFORMS.get(str(raw["operation"]))
            if platform:
                signals.add(platform)
            if raw["platform"]:
                signals.add(str(raw["platform"]))
            if not signals and raw["slot_id"] in slot_platforms:
                signals.add(slot_platforms[int(raw["slot_id"])])
            if (
                raw["slot_account_id"] is not None
                and int(raw["slot_account_id"]) != old_id
            ):
                _fail(f"v18 raw/slot account mismatch: {raw['id']}")
            if _single_platform(signals, f"provider_raw:{raw['id']}") == "xiaohongshu":
                updates["provider_raw_responses"].append(
                    {
                        "id": int(raw["id"]),
                        "old_account_id": old_id,
                        "new_account_id": new_id,
                    }
                )
    for rows in updates.values():
        rows.sort(key=lambda row: int(row["id"]))
    return {
        "schema_version": "dcar-v18-account-lineage-v1",
        "source_version": 17,
        "candidate_version": 18,
        "max_source_account_id": max_account_id,
        "source_sequences": sequences,
        "splits": splits,
        "historical_mixed_slots": historical_mixed_slots,
        "updates": updates,
        "counts": {table: len(rows) for table, rows in updates.items()},
    }


def _row_digest(rows: Any) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        digest.update(
            json.dumps(
                list(row),
                ensure_ascii=False,
                separators=(",", ":"),
                default=lambda value: {"bytes_hex": bytes(value).hex()},
            ).encode("utf-8")
        )
        digest.update(b"\n")
        count += 1
    return count, digest.hexdigest()


def _rows(
    connection: sqlite3.Connection, table: str, *, max_account_id: int | None = None
) -> Any:
    columns = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    order = [
        str(row["name"])
        for row in sorted(columns, key=lambda row: int(row["pk"]))
        if row["pk"]
    ]
    if not order:
        order = [str(row["name"]) for row in columns]
    quoted_order = ",".join('"' + value + '"' for value in order)
    clause = (
        " WHERE id<=?" if table == "accounts" and max_account_id is not None else ""
    )
    params = (max_account_id,) if clause else ()
    return connection.execute(
        f'SELECT * FROM "{table}"{clause} ORDER BY {quoted_order}', params
    )


def _preserved_columns_state(
    connection: sqlite3.Connection, plan: dict[str, Any]
) -> dict[str, Any]:
    from . import storage

    result = {}
    for table in ("accounts", *plan["updates"]):
        columns = storage._table_columns(connection, table)
        excluded = columns.index("account_id") if table != "accounts" else None
        rows = _rows(connection, table, max_account_id=plan["max_source_account_id"])
        result[table] = _row_digest(
            [value for index, value in enumerate(row) if index != excluded]
            for row in rows
        )
    return result


def migrate(connection: sqlite3.Connection) -> dict[str, Any]:
    from . import storage

    storage._require_initialization_safety(connection)
    if connection.in_transaction:
        _fail("v18 migration requires no active transaction")
    if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
        _fail("v18 migration requires foreign_keys=ON")
    legacy_alter = int(connection.execute("PRAGMA legacy_alter_table").fetchone()[0])
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("PRAGMA legacy_alter_table=ON")
    try:
        connection.execute("BEGIN IMMEDIATE")
        plan = migration_plan(connection)
        before = _preserved_columns_state(connection, plan)
        storage._migration_checkpoint("v18_preflight_complete")
        applied_at = storage.now_utc()
        connection.execute("ALTER TABLE accounts RENAME TO accounts_v17_old")
        connection.execute(ACCOUNT_SQL)
        connection.execute("INSERT INTO accounts SELECT * FROM accounts_v17_old")
        connection.execute("DROP TABLE accounts_v17_old")
        for split in plan["splits"]:
            connection.execute(
                "INSERT INTO accounts(id,phone,phone_normalized,operator_name,account_type,"
                "content_direction,enabled,created_at,updated_at) "
                "SELECT ?,phone,phone_normalized,operator_name,account_type,"
                "content_direction,enabled,?,? FROM accounts WHERE id=?",
                (
                    split["new_account_id"],
                    applied_at,
                    applied_at,
                    split["old_account_id"],
                ),
            )
        storage._migration_checkpoint("v18_accounts_split")
        for table, rows in plan["updates"].items():
            for row in rows:
                cursor = connection.execute(
                    f'UPDATE "{table}" SET account_id=? WHERE id=? AND account_id=?',
                    (row["new_account_id"], row["id"], row["old_account_id"]),
                )
                if cursor.rowcount != 1:
                    _fail(f"v18 rekey lost ownership for {table}:{row['id']}")
        storage._migration_checkpoint("v18_references_rekeyed")
        connection.execute(
            "ALTER TABLE account_platform_identities RENAME TO account_platform_identities_v17_old"
        )
        connection.execute(IDENTITY_SQL)
        connection.execute(
            "INSERT INTO account_platform_identities SELECT * FROM account_platform_identities_v17_old"
        )
        connection.execute("DROP TABLE account_platform_identities_v17_old")
        for statement in _statements(MATRIX_SCHEMA_SQL):
            connection.execute(statement)
        desired_sequences = dict(plan["source_sequences"])
        if plan["splits"]:
            desired_sequences["accounts"] = plan["splits"][-1]["new_account_id"]
        for table in ("accounts", "account_platform_identities"):
            connection.execute("DELETE FROM sqlite_sequence WHERE name=?", (table,))
            if table in desired_sequences:
                connection.execute(
                    "INSERT INTO sqlite_sequence(name,seq) VALUES (?,?)",
                    (table, desired_sequences[table]),
                )
        storage._migration_checkpoint("v18_schema_created")
        connection.execute(
            "INSERT INTO schema_migrations(version,name,applied_at) VALUES (18,?,?)",
            (MIGRATION_NAME, applied_at),
        )
        connection.execute("PRAGMA user_version=18")
        validate_structure(connection)
        if _preserved_columns_state(connection, plan) != before:
            _fail("v18 changed a protected old-row projection")
        if _sequences(connection) != desired_sequences:
            _fail("v18 changed a protected sequence")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            _fail("v18 migration has foreign-key violations")
        if str(connection.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
            _fail("v18 migration failed quick_check")
        storage._migration_checkpoint("v18_before_commit")
        connection.commit()
        return plan
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.execute(f"PRAGMA legacy_alter_table={legacy_alter}")
        connection.execute("PRAGMA foreign_keys=ON")


def _schema_objects(connection: sqlite3.Connection) -> dict[tuple[str, str], str]:
    return {
        (str(row[0]), str(row[1])): str(row[2])
        for row in connection.execute(
            "SELECT type,name,sql FROM sqlite_master "
            "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY type,name"
        )
    }


def validate_lineage(
    source: sqlite3.Connection, candidate: sqlite3.Connection
) -> dict[str, Any]:
    """Independent migration proof, optionally followed by one proven bootstrap."""
    from . import storage

    plan = migration_plan(source)
    storage.require_schema_compatibility(candidate, supported_versions=frozenset({18}))
    source_tables = storage._table_names(source)
    candidate_tables = storage._table_names(candidate)
    if (
        candidate_tables - source_tables != set(NEW_TABLES)
        or source_tables - candidate_tables
    ):
        _fail("v18 lineage has unexpected added/removed tables")
    bootstrap: dict[str, Any] | None = None
    appended: dict[str, set[int]] = {}
    appended_references: set[tuple[int, str, str]] = set()
    if candidate.execute("SELECT COUNT(*) FROM account_metric_observations").fetchone()[0]:
        _fail("v18 bare migration populated account_metric_observations")
    if candidate.execute("SELECT COUNT(*) FROM account_roster_snapshots").fetchone()[0]:
        from .account_roster import RosterError, validate_bootstrap_extension

        try:
            bootstrap = validate_bootstrap_extension(source, candidate)
        except RosterError as error:
            _fail(f"v18 bootstrap lineage rejected: {error}")
        assert bootstrap is not None
        allowed_tables = {
            "provider_raw_responses", "scheduler_runs", "account_provider_references",
            "account_roster_snapshots",
        }
        if set(bootstrap["added_rows"]) != allowed_tables:
            _fail("bootstrap has an undeclared append domain")
        for table, identifiers in bootstrap["added_rows"].items():
            if table == "account_provider_references":
                appended_references = {
                    (int(key[0]), str(key[1]), str(key[2])) for key in identifiers
                }
                if len(appended_references) != len(identifiers):
                    _fail("bootstrap has duplicate composite provider-reference keys")
                continue
            appended[table] = {int(value) for value in identifiers}
            previous = max(
                plan["source_sequences"].get(table, 0),
                int(source.execute(f'SELECT COALESCE(MAX(id),0) FROM "{table}"').fetchone()[0])
                if table in source_tables else 0,
            )
            if (
                len(appended[table]) != len(identifiers)
                or sorted(appended[table]) != list(range(previous + 1, previous + len(identifiers) + 1))
            ):
                _fail(f"bootstrap has unexpected appended identities in {table}")
    else:
        for table in NEW_TABLES:
            if int(candidate.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]) != 0:
                _fail(f"v18 bare migration populated {table}")
    old_migrations = [
        tuple(row)
        for row in source.execute("SELECT * FROM schema_migrations ORDER BY version")
    ]
    new_migrations = [
        tuple(row)
        for row in candidate.execute("SELECT * FROM schema_migrations ORDER BY version")
    ]
    if (
        len(new_migrations) != len(old_migrations) + 1
        or new_migrations[:-1] != old_migrations
        or new_migrations[-1][:2] != (18, MIGRATION_NAME)
        or not new_migrations[-1][2]
    ):
        _fail("v18 lineage changed historical migration records")
    applied_at = new_migrations[-1][2]
    retained: dict[str, Any] = {}
    for table in sorted(source_tables):
        source_columns = storage._table_columns(source, table)
        if storage._table_columns(candidate, table) != source_columns:
            _fail(f"v18 changed column order or names for {table}")
        changes = {
            int(row["id"]): int(row["new_account_id"])
            for row in plan["updates"].get(table, [])
        }
        account_index = source_columns.index("account_id") if changes else None
        id_index = source_columns.index("id") if changes else None
        source_hash = hashlib.sha256()
        expected_hash = hashlib.sha256()
        source_count = 0
        for row in _rows(source, table):
            values = list(row)
            original = (
                json.dumps(
                    values,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=lambda value: {"bytes_hex": bytes(value).hex()},
                ).encode("utf-8")
                + b"\n"
            )
            source_hash.update(original)
            if changes:
                assert id_index is not None and account_index is not None
                if int(values[id_index]) in changes:
                    values[account_index] = changes[int(values[id_index])]
            expected_hash.update(
                json.dumps(
                    values,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=lambda value: {"bytes_hex": bytes(value).hex()},
                ).encode("utf-8")
                + b"\n"
            )
            source_count += 1
        if table == "schema_migrations":
            candidate_rows = (
                row for row in _rows(candidate, table) if int(row["version"]) != 18
            )
        else:
            candidate_rows = _rows(
                candidate, table, max_account_id=plan["max_source_account_id"]
            )
            if table in appended:
                candidate_rows = (
                    row for row in candidate_rows if int(row["id"]) not in appended[table]
                )
            if table == "account_provider_references" and appended_references:
                candidate_rows = (
                    row for row in candidate_rows
                    if (int(row["account_identity_id"]), str(row["provider"]), str(row["reference_kind"]))
                    not in appended_references
                )
        candidate_count, candidate_hash = _row_digest(candidate_rows)
        if (
            candidate_count != source_count
            or candidate_hash != expected_hash.hexdigest()
        ):
            _fail(
                f"v18 lineage changed protected rows or unauthorized fields in {table}"
            )
        retained[table] = {
            "columns": source_columns,
            "row_count": source_count,
            "source_sha256": source_hash.hexdigest(),
            "expected_candidate_sha256": expected_hash.hexdigest(),
            "candidate_sha256": candidate_hash,
            "changed_rows": len(changes),
            "allowed_changed_columns": ["account_id"] if changes else [],
        }
    accounts_columns = storage._table_columns(source, "accounts")
    id_index = accounts_columns.index("id")
    created_index = accounts_columns.index("created_at")
    updated_index = accounts_columns.index("updated_at")
    for split in plan["splits"]:
        original = source.execute(
            "SELECT * FROM accounts WHERE id=?", (split["old_account_id"],)
        ).fetchone()
        actual = candidate.execute(
            "SELECT * FROM accounts WHERE id=?", (split["new_account_id"],)
        ).fetchone()
        expected = list(original)
        expected[id_index] = split["new_account_id"]
        expected[created_index] = applied_at
        expected[updated_index] = applied_at
        if actual is None or list(actual) != expected:
            _fail(
                f"v18 new account {split['new_account_id']} does not preserve parent source"
            )
    expected_total = retained["accounts"]["row_count"] + len(plan["splits"])
    if (
        int(candidate.execute("SELECT COUNT(*) FROM accounts").fetchone()[0])
        != expected_total
    ):
        _fail("v18 lineage added unexpected accounts")
    expected_sequences = dict(plan["source_sequences"])
    if plan["splits"]:
        expected_sequences["accounts"] = plan["splits"][-1]["new_account_id"]
    for table, identifiers in appended.items():
        if identifiers:
            expected_sequences[table] = max(identifiers)
    candidate_sequences = _sequences(candidate)
    if candidate_sequences != expected_sequences:
        _fail("v18 lineage changed unauthorized sqlite_sequence values")
    source_objects = _schema_objects(source)
    candidate_objects = _schema_objects(candidate)
    allowed_changed = {("table", "accounts"), ("table", "account_platform_identities")}
    changed = {
        key
        for key in source_objects
        if source_objects[key] != candidate_objects.get(key)
    }
    if changed != allowed_changed:
        _fail(f"v18 schema lineage changed unexpected old objects: {sorted(changed)}")
    added = set(candidate_objects) - set(source_objects)
    expected_added = set(expected_schema_objects()) - set(source_objects)
    if added != expected_added:
        _fail("v18 schema lineage additions differ from contract")
    if candidate.execute("PRAGMA foreign_key_check").fetchall():
        _fail("v18 candidate has foreign-key violations")
    for connection, label in ((source, "source"), (candidate, "candidate")):
        if str(connection.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
            _fail(f"v18 {label} failed integrity_check")
    schema_objects = {
        "retained_count": len(source_objects) - len(changed),
        "changed": [list(key) for key in sorted(changed)],
        "added": [list(key) for key in sorted(added)],
        "removed": [],
        "source_sha256": hashlib.sha256(
            json.dumps(
                [[*key, value] for key, value in sorted(source_objects.items())],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        "candidate_sha256": hashlib.sha256(
            json.dumps(
                [[*key, value] for key, value in sorted(candidate_objects.items())],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    }
    result = {
        "schema_version": "dcar-v18-offline-allowed-differences-v1",
        "source_table_count": len(source_tables),
        "candidate_table_count": len(candidate_tables),
        "retained_table_count": len(retained),
        "retained_tables": retained,
        "added_tables": sorted(NEW_TABLES),
        "removed_tables": [],
        "appended_migration_versions": [18],
        "sqlite_sequence": candidate_sequences,
        "schema_objects": schema_objects,
        "account_lineage": plan,
    }
    if bootstrap is not None:
        result["bootstrap"] = bootstrap
    return result
