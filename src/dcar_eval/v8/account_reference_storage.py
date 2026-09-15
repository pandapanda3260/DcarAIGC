"""Persist already verified references across the schema21/22 storage boundary."""
from __future__ import annotations

import sqlite3


def store_reference(connection: sqlite3.Connection, *, account_identity_id: int,
                    platform: str, provider: str, reference_kind: str,
                    reference_value: str, source_raw_response_id: int | None,
                    created_at: str, updated_at: str, update_existing: bool = False) -> None:
    """Keep callers' matching/conflict policy; derive platform from the bound identity."""
    identity = connection.execute("SELECT platform FROM account_platform_identities WHERE id=?",
                                  (account_identity_id,)).fetchone()
    if identity is None or identity[0] != platform:
        raise ValueError("Provider reference platform differs from its bound identity")
    if update_existing:
        existing = connection.execute("SELECT provider FROM account_provider_references "
            "WHERE account_identity_id=? AND lower(provider)=lower(?) AND reference_kind=?",
            (account_identity_id, provider, reference_kind)).fetchall()
        if len(existing) > 1:
            raise ValueError("Multiple provider reference spellings require an explicit repair")
        if existing:
            provider = existing[0][0]
    fields = {"account_identity_id": account_identity_id, "provider": provider,
              "reference_kind": reference_kind, "reference_value": reference_value,
              "source_raw_response_id": source_raw_response_id,
              "created_at": created_at, "updated_at": updated_at}
    if "platform" in {row[1] for row in connection.execute("PRAGMA table_info(account_provider_references)")}:
        fields["platform"] = identity[0]
    sql = ("INSERT INTO account_provider_references(" + ",".join(fields) + ") VALUES("
           + ",".join("?" for _ in fields) + ")")
    if update_existing:
        sql += (" ON CONFLICT(account_identity_id,provider,reference_kind) DO UPDATE SET "
                "reference_value=excluded.reference_value,source_raw_response_id=excluded.source_raw_response_id,"
                "updated_at=excluded.updated_at")
    connection.execute(sql, tuple(fields.values()))
