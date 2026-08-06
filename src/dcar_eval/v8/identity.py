"""Platform-level pseudonymous identity and legacy score compatibility.

Implements the v8.4 dual-key contract:

* ``content-user-hmac-v1`` (workflow.privacy.CommentHasher) keeps feeding the
  legacy acquisition-score chain through ``comment_user_scores``.
* ``platform-user-hmac-v2`` (:class:`PlatformUserHasher`) is the only key that
  enters the interaction-user domain and stays stable across contents on the
  same platform.

Raw platform user ids exist in memory only while both keys are derived; they
are never written to the database, evidence payloads or logs.
"""

from __future__ import annotations

import hashlib
import hmac
import importlib
import os
import secrets
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .storage import PLATFORM_USER_KEY_VERSION, PROJECT_ROOT


DEFAULT_PLATFORM_SALT_PATH = PROJECT_ROOT / "data/cache/.platform_user_salt"
LEGACY_SCORE_MAX_COMMENTS_PER_USER = 3
LEGACY_SCORE_TEXT_SEPARATOR = "；"


class PlatformUserHasher:
    """Stable platform-scoped HMAC pseudonyms (``platform-user-hmac-v2``).

    The key message is version-prefixed and ``\\0``-separated so the same raw
    uid can never collide across versions or platforms by concatenation.
    """

    def __init__(self, salt_path: Path = DEFAULT_PLATFORM_SALT_PATH) -> None:
        self.salt_path = salt_path
        self.salt = self._load_or_create()

    def _load_or_create(self) -> bytes:
        self.salt_path.parent.mkdir(parents=True, exist_ok=True)
        if self.salt_path.exists():
            value = self.salt_path.read_bytes()
            if len(value) < 32:
                raise ValueError(
                    "platform user salt must contain at least 32 bytes"
                )
            os.chmod(self.salt_path, 0o600)
            return value
        value = secrets.token_bytes(32)
        descriptor = os.open(
            self.salt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(value)
        return value

    def user_key(self, platform: str, raw_platform_user_id: str) -> str:
        if not platform or not raw_platform_user_id:
            return ""
        message = (
            f"{PLATFORM_USER_KEY_VERSION}\0{platform}\0{raw_platform_user_id}"
        ).encode("utf-8", "replace")
        return "P" + hmac.new(self.salt, message, hashlib.sha256).hexdigest()


def comment_identity_key(
    *,
    platform_comment_id: Optional[str],
    pseudonymous_user_key: Optional[str],
    body: str,
    published_at: Optional[str],
) -> Optional[str]:
    """Stable per-comment identity used against page overlap and replay."""

    normalized_comment_id = str(platform_comment_id or "")
    if normalized_comment_id:
        return f"cid:{normalized_comment_id}"
    if not body:
        return None
    digest = hashlib.sha256(
        "\0".join(
            (str(pseudonymous_user_key or ""), body, str(published_at or ""))
        ).encode("utf-8", "replace")
    ).hexdigest()
    return f"sha:{digest}"


def ensure_interaction_user(
    connection: sqlite3.Connection,
    *,
    platform: str,
    pseudonymous_user_key: Optional[str],
    seen_at: str,
) -> Optional[int]:
    """Insert-or-touch the platform-level interaction user; return its id."""

    key = str(pseudonymous_user_key or "")
    if not platform or not key:
        return None
    connection.execute(
        """
        INSERT INTO interaction_users(
            platform, pseudonymous_user_key, key_version,
            first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(platform, key_version, pseudonymous_user_key) DO UPDATE SET
            first_seen_at=MIN(interaction_users.first_seen_at, excluded.first_seen_at),
            last_seen_at=MAX(interaction_users.last_seen_at, excluded.last_seen_at)
        """,
        (platform, key, PLATFORM_USER_KEY_VERSION, seen_at, seen_at),
    )
    row = connection.execute(
        """
        SELECT id FROM interaction_users
        WHERE platform=? AND key_version=? AND pseudonymous_user_key=?
        """,
        (platform, PLATFORM_USER_KEY_VERSION, key),
    ).fetchone()
    return int(row[0]) if row is not None else None


def insert_comment_rows(
    connection: sqlite3.Connection,
    *,
    platform: str,
    evidence_version_id: int,
    comments: Iterable[Mapping[str, Any]],
    captured_at: str,
) -> int:
    """Insert sanitized comment rows linked to their interaction users.

    Comments without a platform-level pseudonym (historical replays) are kept
    with a NULL ``interaction_user_id``; their identity key falls back to the
    content-scoped v1 key so page replays still deduplicate deterministically.
    """

    inserted = 0
    for item in comments:
        body = str(item.get("body") or "")
        pseudonymous_user_key = str(item.get("pseudonymous_user_key") or "")
        identity_key = str(item.get("comment_identity_key") or "") or (
            comment_identity_key(
                platform_comment_id=item.get("platform_comment_id"),
                pseudonymous_user_key=(
                    pseudonymous_user_key
                    or str(item.get("anonymous_user_key") or "")
                ),
                body=body,
                published_at=item.get("published_at"),
            )
        )
        interaction_user_id = ensure_interaction_user(
            connection,
            platform=platform,
            pseudonymous_user_key=pseudonymous_user_key,
            seen_at=str(item.get("published_at") or captured_at),
        )
        cursor = connection.execute(
            """
            INSERT INTO comments(
                evidence_version_id, platform_comment_id, anonymous_user_key,
                body, published_at, like_count, parent_comment_id,
                interaction_user_id, comment_identity_key, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')
            ON CONFLICT DO NOTHING
            """,
            (
                evidence_version_id,
                item.get("platform_comment_id") or None,
                item.get("anonymous_user_key") or None,
                body,
                item.get("published_at"),
                item.get("like_count"),
                item.get("parent_comment_id"),
                interaction_user_id,
                identity_key,
            ),
        )
        inserted += int(cursor.rowcount > 0)
    return inserted


def legacy_user_score_rows(
    comments: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Deterministic ``legacy-audience-action-v1`` rows for the v1 key domain.

    Per content-scoped v1 user: order comments by publication time and stable
    comment keys, keep at most the first three distinct texts (the historical
    batch semantics), join them and run the frozen 0/30/70/100 rules once.
    """

    scoring = importlib.import_module("analyze_douyin_tikhub_v6")
    grouped: Dict[str, List[Mapping[str, Any]]] = {}
    for comment in comments:
        key = str(comment.get("anonymous_user_key") or "")
        body = str(comment.get("body") or "")
        if key and body:
            grouped.setdefault(key, []).append(comment)
    rows: List[Dict[str, Any]] = []
    for key in sorted(grouped):
        ordered = sorted(
            grouped[key],
            key=lambda item: (
                str(item.get("published_at") or ""),
                str(item.get("comment_identity_key") or ""),
                str(item.get("platform_comment_id") or ""),
                str(item.get("body") or ""),
            ),
        )
        texts: List[str] = []
        for item in ordered:
            body = str(item.get("body") or "")
            if body and body not in texts:
                texts.append(body)
            if len(texts) >= LEGACY_SCORE_MAX_COMMENTS_PER_USER:
                break
        combined = LEGACY_SCORE_TEXT_SEPARATOR.join(texts)
        rows.append(
            {
                "anonymous_user_key": key,
                "audience_automotive_score": scoring.audience_user_score(
                    combined, context_automotive=True
                ),
                "action_intent_score": scoring.action_user_score(
                    combined, context_automotive=True
                ),
            }
        )
    return rows
