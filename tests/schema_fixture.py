from __future__ import annotations

import sqlite3
from pathlib import Path

import v8.storage as storage


_V8_FROZEN_SCHEMA = (
    Path(__file__).resolve().parent / "fixtures" / "schema_v8_frozen.sql"
).read_text(encoding="utf-8")

_V9_FROZEN_SCHEMA = (
    Path(__file__).resolve().parent / "fixtures" / "schema_v9_frozen.sql"
).read_text(encoding="utf-8")

_MIGRATIONS = {
    10: storage._migrate_v9_to_v10,
    11: storage._migrate_v10_to_v11,
    12: storage._migrate_v11_to_v12,
}


def initialize_historical_schema(
    connection: sqlite3.Connection,
    *,
    target_version: int,
) -> None:
    """Build a genuine historical schema without downgrading the current one.

    Destructive migrations make a current-schema-then-DROP fixture unsafe: v16,
    for example, removed the manual-review tables and rebuilt
    ``evaluation_versions``.  Starting from the frozen v9 schema and applying
    the real forward migrations keeps later schema additions from leaking into
    v11/v12 fixtures and lets ``initialize_database`` exercise the full forward
    ladder.
    """

    if target_version not in {8, *_MIGRATIONS, 16, 17}:
        raise ValueError(f"unsupported historical fixture version: {target_version}")
    if storage._table_names(connection):
        raise AssertionError("historical schema fixture requires an empty database")

    if target_version == 8:
        # Exact v8 DDL from e1ad3a4, before release-bound v9 reconstruction.
        connection.executescript(_V8_FROZEN_SCHEMA)
        connection.execute(
            "INSERT INTO schema_migrations(version,name,applied_at) VALUES (8,?,?)",
            ("append-only-review-reopen-audit", "2026-08-02T00:00:00Z"),
        )
        connection.execute("PRAGMA user_version=8")
        connection.commit()
        return

    if target_version in {16, 17}:
        connection.executescript(storage.frozen_schema_sql(target_version))
        historical_names = {
            9: "release-bound-evaluation-schema",
            10: "audience-interaction-user-domain",
            11: "interaction-user-v1-fallback-keys",
            12: "append-only-metric-observations",
            13: "scheduler-run-attempt-history",
            14: "spu-audience-scene-domain",
            15: "spu-llm-assist",
            16: "remove-manual-review",
            17: "optional-account-phone",
        }
        connection.executemany(
            "INSERT INTO schema_migrations(version,name,applied_at) VALUES (?,?,?)",
            (
                (version, name, "2026-08-28T00:00:00Z")
                for version, name in historical_names.items()
                if version <= target_version
            ),
        )
        connection.execute(f"PRAGMA user_version={target_version}")
        connection.commit()
        storage.require_schema_compatibility(
            connection, supported_versions=frozenset({target_version})
        )
        return

    connection.executescript(_V9_FROZEN_SCHEMA)
    connection.executemany(
        """
        INSERT INTO schema_migrations(version,name,applied_at)
        VALUES (?,?,?)
        """,
        (
            (8, "append-only-review-reopen-audit", "2026-08-02T00:00:00Z"),
            (9, "release-bound-evaluation-schema", "2026-08-04T00:00:00Z"),
        ),
    )
    connection.execute("PRAGMA user_version=9")
    connection.commit()

    for version in range(10, target_version + 1):
        _MIGRATIONS[version](connection)

    actual = storage.require_schema_compatibility(
        connection,
        supported_versions=frozenset({target_version}),
    )
    if actual != target_version:
        raise AssertionError(
            f"historical fixture version drifted: expected {target_version}, got {actual}"
        )
