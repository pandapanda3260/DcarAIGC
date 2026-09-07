"""Immutable system-managed account roster mutations for TikHub mode."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .account_roster import (
    MATRIX_SOURCE_FAMILY,
    SYSTEM_CONTRACT_VERSION,
    SYSTEM_SOURCE_FAMILY,
    RosterError,
    accept_candidate,
    get_current_members,
    latest_family_snapshot,
    normalize_member,
    prepare_candidate,
)
from .storage import now_utc, write_lock


DEFAULT_ORGANIZATION = "dcar-local-managed"


@contextmanager
def _atomic_mutation(connection: sqlite3.Connection) -> Iterator[None]:
    nested = connection.in_transaction
    # A standalone BEGIN IMMEDIATE must hold the process write lock for its
    # whole span (see storage.write_lock); a nested SAVEPOINT already runs
    # inside the caller's locked transaction.
    with nullcontext() if nested else write_lock():
        connection.execute("SAVEPOINT system_roster" if nested else "BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            if nested:
                connection.execute("ROLLBACK TO system_roster")
                connection.execute("RELEASE system_roster")
            else:
                connection.rollback()
            raise
        else:
            if nested:
                connection.execute("RELEASE system_roster")
            else:
                connection.commit()


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _parse_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise RosterError(
            "invalid_source_time", "Timezone-aware source time is required"
        ) from error
    if parsed.tzinfo is None:
        raise RosterError(
            "invalid_source_time", "Timezone-aware source time is required"
        )
    return parsed.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _seal_time(connection: sqlite3.Connection, requested: str | None) -> str:
    value = _parse_time(requested or now_utc())
    current = latest_family_snapshot(connection, SYSTEM_SOURCE_FAMILY)
    if current is not None:
        previous = _parse_time(str(current["source_captured_at"]))
        if value <= previous:
            value = previous + timedelta(microseconds=1)
    return _timestamp(value)


def _member_input(value: Mapping[str, Any]) -> dict[str, Any]:
    metadata = dict(value.get("metadata") or {})
    sec_user_id = value.get("sec_user_id", metadata.pop("sec_user_id", None))
    metadata.pop("nickname", None)
    return {
        "platform": value.get("platform"),
        "uid": value.get("uid"),
        "nickname": value.get("nickname") or "",
        "profile_ref": value.get("profile_ref"),
        "sec_user_id": sec_user_id,
        "monitoring_status": value.get("monitoring_status", "unknown"),
        "authorization_status": value.get("authorization_status", "unknown"),
        "monitoring_started_at": value.get("monitoring_started_at"),
        "metadata": metadata,
    }


def _normalized_input(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = normalize_member(value, source_family=SYSTEM_SOURCE_FAMILY)
    return _member_input(normalized)


def current_system_members(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return the newest accepted system-family roster, including pending rosters."""

    snapshot = latest_family_snapshot(connection, SYSTEM_SOURCE_FAMILY)
    if snapshot is None:
        return []
    return [
        _member_input(value)
        for value in get_current_members(
            connection,
            snapshot_id=int(snapshot["id"]),
            source_family=SYSTEM_SOURCE_FAMILY,
        )
    ]


def seal_system_members(
    connection: sqlite3.Connection,
    members: Sequence[Mapping[str, Any]],
    *,
    raw_root: Path,
    actor: str,
    reason: str,
    organization: str = DEFAULT_ORGANIZATION,
    sealed_at: str | None = None,
) -> dict[str, Any]:
    """Accept one complete system roster; activation remains a separate action."""

    actor = str(actor).strip()
    reason = str(reason).strip()
    organization = str(organization).strip()
    if not actor or not reason:
        raise RosterError(
            "system_roster_reason_required", "System roster actor and reason are required"
        )
    if not organization:
        raise RosterError(
            "incomplete_scope", "System roster organization is required"
        )
    normalized = sorted(
        (_normalized_input(value) for value in members),
        key=lambda value: (str(value["platform"]), str(value["uid"])),
    )
    timestamp = _seal_time(connection, sealed_at)
    manifest = {
        # Equal membership after pause/resume is a new administrative event,
        # not a replay of an older accepted source.
        "source_captured_at": timestamp,
        "contract_version": SYSTEM_CONTRACT_VERSION,
        "members": normalized,
    }
    source = _canonical(manifest).encode("utf-8")
    source_sha256 = hashlib.sha256(source).hexdigest()
    previous = latest_family_snapshot(connection, SYSTEM_SOURCE_FAMILY)
    seal_id = hashlib.sha256(
        _canonical(
            {
                "actor": actor,
                "reason": reason,
                "sealed_at": timestamp,
                "previous_snapshot_id": previous["id"] if previous else None,
                "source_sha256": source_sha256,
            }
        ).encode("utf-8")
    ).hexdigest()
    candidate = prepare_candidate(
        connection,
        {
            "source_type": "system_managed",
            "scope": {
                "organization": organization,
                "coverage": "full",
                "account_scope": "all_managed_accounts",
                "platforms": [
                    "douyin",
                    "xiaohongshu",
                    "wechat_channels",
                    "kuaishou",
                ],
            },
            "source_captured_at": timestamp,
            "source_evidence": {
                "kind": "system_roster_seal",
                "evidence_kind": "system_roster_manifest",
                "source_format": "system-roster-json-v1",
                "source_name": "system-roster.json",
                "seal_id": seal_id,
                "sealed_at": timestamp,
                "scope_evidence": f"{actor}: {reason}",
                "source_sha256": source_sha256,
            },
            "members": normalized,
            "declared_count": len(normalized),
            "pagination": {
                "expected_pages": 1,
                "pages": [1],
                "terminal": True,
                "declared_totals": [len(normalized)],
            },
        },
        source_bytes=source,
        raw_root=raw_root,
        observed_at=timestamp,
    )
    accepted = accept_candidate(
        connection, int(candidate["candidate_id"]), accepted_at=timestamp
    )
    return {
        **accepted,
        "source_family": SYSTEM_SOURCE_FAMILY,
        "activation_status": "pending_activation",
    }


def _upsert_system_members_in_transaction(
    connection: sqlite3.Connection,
    rows: Sequence[Mapping[str, Any]],
    *,
    raw_root: Path,
    actor: str,
    reason: str,
    organization: str = DEFAULT_ORGANIZATION,
    sealed_at: str | None = None,
) -> dict[str, Any]:
    """Merge valid UID rows and seal exactly one new full-roster snapshot."""

    current = {
        (str(value["platform"]), str(value["uid"])): value
        for value in current_system_members(connection)
    }
    last_by_key: dict[tuple[str, str], int] = {}
    keys: list[tuple[str, str] | None] = []
    for index, row in enumerate(rows):
        platform = str(row.get("platform") or "")
        uid = str(row.get("uid") or "")
        key = (platform, uid) if platform and uid else None
        keys.append(key)
        if key is not None:
            last_by_key[key] = index

    results: list[dict[str, Any]] = []
    changed = False
    for index, row in enumerate(rows):
        key = keys[index]
        if key is not None and last_by_key[key] != index:
            results.append(
                {"row": index + 1, "status": "duplicate_in_file", "reason": "later row wins"}
            )
            continue
        base = current.get(key, {}) if key is not None else {}
        merged = {**base, **dict(row)}
        if isinstance(base.get("metadata"), Mapping) or isinstance(
            row.get("metadata"), Mapping
        ):
            merged["metadata"] = {
                **dict(base.get("metadata") or {}),
                **dict(row.get("metadata") or {}),
            }
        try:
            normalized = _normalized_input(merged)
        except RosterError as error:
            results.append(
                {"row": index + 1, "status": "rejected", "reason": error.code}
            )
            continue
        normalized_key = (str(normalized["platform"]), str(normalized["uid"]))
        previous = current.get(normalized_key)
        status = "inserted" if previous is None else "updated"
        if previous is not None and _canonical(previous) == _canonical(normalized):
            status = "unchanged"
        else:
            current[normalized_key] = normalized
            changed = True
        results.append({"row": index + 1, "status": status, "reason": ""})

    if not changed:
        return {
            "status": "unchanged",
            "source_family": SYSTEM_SOURCE_FAMILY,
            "snapshot_id": (
                latest_family_snapshot(connection, SYSTEM_SOURCE_FAMILY) or {}
            ).get("id"),
            "results": results,
        }
    accepted = seal_system_members(
        connection,
        list(current.values()),
        raw_root=raw_root,
        actor=actor,
        reason=reason,
        organization=organization,
        sealed_at=sealed_at,
    )
    return {**accepted, "results": results}


def upsert_system_members(
    connection: sqlite3.Connection,
    rows: Sequence[Mapping[str, Any]],
    *,
    raw_root: Path,
    actor: str,
    reason: str,
    organization: str = DEFAULT_ORGANIZATION,
    sealed_at: str | None = None,
) -> dict[str, Any]:
    """Serialize read-merge-seal so concurrent API writes cannot lose members."""

    with _atomic_mutation(connection):
        return _upsert_system_members_in_transaction(
            connection,
            rows,
            raw_root=raw_root,
            actor=actor,
            reason=reason,
            organization=organization,
            sealed_at=sealed_at,
        )


def _remove_system_member_in_transaction(
    connection: sqlite3.Connection,
    account_id: int,
    *,
    raw_root: Path,
    actor: str,
    reason: str,
    organization: str = DEFAULT_ORGANIZATION,
    sealed_at: str | None = None,
) -> dict[str, Any]:
    """Remove one account from the next system roster without deleting history."""

    members = current_system_members(connection)
    identities = {
        int(row["account_id"]): (str(row["platform"]), str(row["uid"]))
        for row in get_current_members(
            connection,
            snapshot_id=int(
                (latest_family_snapshot(connection, SYSTEM_SOURCE_FAMILY) or {"id": 0})[
                    "id"
                ]
            ),
            source_family=SYSTEM_SOURCE_FAMILY,
        )
    } if members else {}
    key = identities.get(account_id)
    if key is None:
        raise RosterError(
            "system_member_not_found", "Account is not in the system-managed roster"
        )
    retained = [
        value
        for value in members
        if (str(value["platform"]), str(value["uid"])) != key
    ]
    return seal_system_members(
        connection,
        retained,
        raw_root=raw_root,
        actor=actor,
        reason=reason,
        organization=organization,
        sealed_at=sealed_at,
    )


def remove_system_member(
    connection: sqlite3.Connection,
    account_id: int,
    *,
    raw_root: Path,
    actor: str,
    reason: str,
    organization: str = DEFAULT_ORGANIZATION,
    sealed_at: str | None = None,
) -> dict[str, Any]:
    """Serialize member lookup and full-roster removal."""

    with _atomic_mutation(connection):
        return _remove_system_member_in_transaction(
            connection,
            account_id,
            raw_root=raw_root,
            actor=actor,
            reason=reason,
            organization=organization,
            sealed_at=sealed_at,
        )


def _bootstrap_system_roster_from_matrix_in_transaction(
    connection: sqlite3.Connection,
    *,
    raw_root: Path,
    actor: str,
    reason: str,
    organization: str = DEFAULT_ORGANIZATION,
    sealed_at: str | None = None,
) -> dict[str, Any]:
    """Create the initial managed roster from UID-complete Matrix membership."""

    if latest_family_snapshot(connection, SYSTEM_SOURCE_FAMILY) is not None:
        raise RosterError(
            "system_roster_already_exists", "System-managed roster is already initialized"
        )
    matrix = latest_family_snapshot(connection, MATRIX_SOURCE_FAMILY)
    if matrix is None:
        raise RosterError("roster_not_ready", "Matrix roster is not available")
    members = get_current_members(
        connection,
        snapshot_id=int(matrix["id"]),
        source_family=MATRIX_SOURCE_FAMILY,
    )
    if any(value.get("uid") is None for value in members):
        raise RosterError(
            "identity_unresolved",
            "Every Matrix member needs a verified UID before system bootstrap",
        )
    return seal_system_members(
        connection,
        [_member_input(value) for value in members],
        raw_root=raw_root,
        actor=actor,
        reason=reason,
        organization=organization,
        sealed_at=sealed_at,
    )


def bootstrap_system_roster_from_matrix(
    connection: sqlite3.Connection,
    *,
    raw_root: Path,
    actor: str,
    reason: str,
    organization: str = DEFAULT_ORGANIZATION,
    sealed_at: str | None = None,
) -> dict[str, Any]:
    """Serialize the one-time Matrix-to-system bootstrap decision."""

    with _atomic_mutation(connection):
        return _bootstrap_system_roster_from_matrix_in_transaction(
            connection,
            raw_root=raw_root,
            actor=actor,
            reason=reason,
            organization=organization,
            sealed_at=sealed_at,
        )
