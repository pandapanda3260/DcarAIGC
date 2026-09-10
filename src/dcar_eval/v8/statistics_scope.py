"""Shared current-statistics scope without changing archived content facts."""

from __future__ import annotations

import re
import sqlite3


def content_statistics_scope_sql(content_alias: str = "c", *, connection: sqlite3.Connection | None = None) -> str:
    """Use the reviewed directory when installed, retaining paused member history.

    Before directory installation the legacy enabled-account scope is preserved.
    Never use this read scope to delete or invalidate historical evidence.
    """

    if not isinstance(content_alias, str) or re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]{0,127}", content_alias
    ) is None:
        raise ValueError("Invalid content SQL alias")
    if connection is not None:
        from .account_directory import has_account_directory
        if has_account_directory(connection):
            return (
                "EXISTS (SELECT 1 FROM account_directory_rows directory_scope "
                f"WHERE directory_scope.account_id={content_alias}.account_id "
                f"AND directory_scope.platform={content_alias}.platform "
                "AND directory_scope.identity_status='existing_verified')"
            )
    account_alias = f"{content_alias}_statistics_account"
    return (
        f"NOT EXISTS (SELECT 1 FROM accounts {account_alias} "
        f"WHERE {account_alias}.id={content_alias}.account_id "
        f"AND {account_alias}.enabled=0)"
    )
