"""Real SQLite accepted-roster fixtures; never imported by production code."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Iterable
from datetime import datetime, timedelta, timezone

from v8.account_roster import current_snapshot, snapshot_by_id
from v8.storage import is_formal_database_path, now_utc


def accept_roster(
    connection: sqlite3.Connection, identity_ids: Iterable[int] | None = None,
    *, accepted_at: str | None = None, activate_profile: bool = True,
) -> dict:
    """Use exactly the existing IDs; do not rewrite legacy synthetic test UIDs."""
    database = Path(connection.execute("PRAGMA database_list").fetchone()[2])
    if is_formal_database_path(database):
        raise AssertionError("Roster fixtures must never target the formal database")
    rows = [dict(row) for row in connection.execute(
        "SELECT * FROM account_platform_identities ORDER BY id"
    )]
    selected = set(identity_ids) if identity_ids is not None else {row["id"] for row in rows}
    rows = [row for row in rows if row["id"] in selected]
    if len(rows) != len(selected):
        raise AssertionError("Fixture requested a non-existing identity")
    at = accepted_at or now_utc()
    snapshot_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(account_roster_snapshots)")
    }
    member_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(account_roster_members)")
    }
    has_families = "source_family" in snapshot_columns and "member_key" in member_columns
    sequence = connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM account_roster_snapshots").fetchone()[0]
    members = [{
        "identity_id": row["id"], "platform": row["platform"],
        "matrix_account_id": f"fixture-matrix-{row['id']}",
        "profile_ref": f"https://fixture.invalid/profile/{row['id']}",
        "uid": row["uid"],
        "member_key": f"matrix:{row['platform']}:fixture-matrix-{row['id']}",
    } for row in rows]
    source = json.dumps(members, sort_keys=True, separators=(",", ":")).encode()
    path = database.parent / f"accepted-roster-fixture-{sequence}.json"
    path.write_bytes(source)
    path.chmod(0o600)
    digest = hashlib.sha256(source).hexdigest()
    # Match the production stable-key digest, separate from source-file bytes.
    keys = sorted((row["platform"], row["matrix_account_id"]) for row in members)
    members_digest = hashlib.sha256(json.dumps(keys, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    scope = {
        "organization": "isolated-test", "coverage": "full",
        "account_scope": "all_added_accounts",
        "platforms": ["douyin", "xiaohongshu", "wechat_channels", "kuaishou"],
    }
    if has_families:
        cursor = connection.execute(
            """INSERT INTO account_roster_snapshots(
                source_family,source_type,scope_key,scope_json,source_instance_id,
                source_captured_at,accepted_at,declared_count,member_count,
                members_sha256,source_sha256,source_path,contract_version,metadata_json)
               VALUES ('matrix','bootstrap_export','isolated-test',?,?,?,?,?,?,?,?,?,
                       'matrix-full-roster-v1','{"fixture":true}')""",
            (json.dumps(scope), f"fixture-{sequence}", at, at, len(rows), len(rows),
             members_digest, digest, str(path)),
        )
    else:
        cursor = connection.execute(
            """INSERT INTO account_roster_snapshots(
                source_type,scope_key,scope_json,source_instance_id,source_captured_at,
                accepted_at,declared_count,member_count,members_sha256,source_sha256,
                source_path,contract_version,metadata_json)
               VALUES ('bootstrap_export','isolated-test',?,?,?,?,?,?,?,?,?,
                       'matrix-full-roster-v1','{"fixture":true}')""",
            (json.dumps(scope), f"fixture-{sequence}", at, at, len(rows), len(rows),
             members_digest, digest, str(path)),
        )
    if cursor.lastrowid is None:
        raise AssertionError("Roster fixture insert returned no snapshot ID")
    snapshot_id = int(cursor.lastrowid)
    if has_families:
        connection.executemany(
            """INSERT INTO account_roster_members(
                snapshot_id,account_identity_id,platform,member_key,uid,
                matrix_account_id,profile_ref,monitoring_status,
                authorization_status,metadata_json)
               VALUES (?,?,?,?,?,?,?,'unknown','unknown','{}')""",
            [
                (
                    snapshot_id,
                    row["identity_id"],
                    row["platform"],
                    row["member_key"],
                    row["uid"],
                    row["matrix_account_id"],
                    row["profile_ref"],
                )
                for row in members
            ],
        )
    else:
        connection.executemany(
            """INSERT INTO account_roster_members(
                snapshot_id,account_identity_id,platform,matrix_account_id,profile_ref,
                monitoring_status,authorization_status,metadata_json)
               VALUES (?,?,?,?,?,'unknown','unknown','{}')""",
            [(snapshot_id, row["identity_id"], row["platform"], row["matrix_account_id"],
              row["profile_ref"]) for row in members],
        )
    if has_families and activate_profile:
        from v8.profile_activations import MATRIX_PROFILE, append_activation

        effective = (
            datetime.fromisoformat(at.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
            # Keep fixture activations strictly ordered while making even a
            # roster accepted one second after a frozen test clock effective
            # before that clock. Production scheduling never uses this shim.
            - timedelta(seconds=2)
            + timedelta(microseconds=sequence)
        ).isoformat(timespec="microseconds").replace("+00:00", "Z")

        activation = append_activation(
            connection,
            profile_id=MATRIX_PROFILE,
            roster_snapshot_id=snapshot_id,
            roster_members_sha256=members_digest,
            effective_at=effective,
            build_receipt_sha256=hashlib.sha256(
                b"isolated-roster-fixture-build"
            ).hexdigest(),
            actor="test-fixture",
            reason="activate isolated Matrix roster fixture",
            metadata={"fixture": True},
            created_at=effective,
        )
        from v8.paid_drain import issue_activation_permit_in_transaction

        issue_activation_permit_in_transaction(
            connection,
            activation_id=int(activation["activation_id"]),
            drain_id=f"fixture-profile:{activation['activation_id']}",
            source_activation_id=int(activation["activation_id"]),
            business_day=datetime.fromisoformat(effective.replace("Z", "+00:00"))
            .astimezone(timezone(timedelta(hours=8)))
            .date()
            .isoformat(),
            planned_effective_at=effective,
            build_receipt_sha256=hashlib.sha256(
                b"isolated-roster-fixture-build"
            ).hexdigest(),
            runtime_root_receipt_sha256=hashlib.sha256(
                b"isolated-roster-fixture-runtime"
            ).hexdigest(),
            now=effective,
        )
    snapshot = (
        snapshot_by_id(connection, snapshot_id)
        if has_families and not activate_profile
        else current_snapshot(connection)
    )
    assert snapshot is not None
    return snapshot
