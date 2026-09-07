"""Shared current-statistics scope without changing archived content facts."""

from __future__ import annotations

import re


def content_statistics_scope_sql(content_alias: str = "c") -> str:
    """Exclude paused accounts while preserving unassociated and enabled history.

    The predicate needs only the content alias, so list counts, detail queries,
    and aggregation denominators can share it without adding account joins.
    Never use this read scope to delete or invalidate historical evidence.
    """

    if not isinstance(content_alias, str) or re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]{0,127}", content_alias
    ) is None:
        raise ValueError("Invalid content SQL alias")
    account_alias = f"{content_alias}_statistics_account"
    return (
        f"NOT EXISTS (SELECT 1 FROM accounts {account_alias} "
        f"WHERE {account_alias}.id={content_alias}.account_id "
        f"AND {account_alias}.enabled=0)"
    )
