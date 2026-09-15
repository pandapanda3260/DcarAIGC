"""Page-scoped, read-only account projections shared by the read service.

Directory search keeps Python's Unicode casefold semantics. Only inexpensive
directory search fields are scanned; full models/metrics are built for one page.
The legacy complete shape remains available, while compact lists load details
explicitly by the stable directory ID (including unlinked directory rows).
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager, nullcontext
import json
from pathlib import Path
import sqlite3
from time import monotonic
from typing import Any, Mapping

from .account_directory import directory_account_items, has_account_directory, _text
from .account_metrics import select_account_metrics
from .account_operating_receipts import load_admission_members, load_update_frequencies
from .account_roster import RosterError, runtime_account_summary, snapshot_by_id, latest_family_snapshot
from .content_scope import canonical_content_predicate
from .evaluation_selectors import active_release
from .operations import ACCOUNT_IDENTITY_STATS_SQL, account_read_model
from .storage import connect, live_wal_read_only_connections

LIST_CONTRACT_VERSION = 1
_SEARCH_FIELDS = ("uid", "nickname", "display_account_id", "phone", "operator_name")


@contextmanager
def _read_transaction(db_path: Path, *, read_only: bool):
    with connect(db_path, read_only=read_only) as connection:
        connection.execute("BEGIN")
        try:
            yield connection
        finally:
            # Count/page IDs, metrics and statuses share one committed snapshot.
            # Release it before a response or cache entry leaves this module.
            connection.rollback()


def _timing(timings: dict[str, Any] | None, name: str, started: float) -> None:
    if timings is not None:
        timings[name] = (monotonic() - started) * 1000


def _context(connection: sqlite3.Connection) -> tuple[dict, dict, dict]:
    active_release(connection)
    return (runtime_account_summary(connection), load_update_frequencies(connection),
            load_admission_members(connection))


def _matching_directory_ids(connection: sqlite3.Connection, payload: Any) -> list[int]:
    query = payload.query.strip().casefold()
    filters = ("platform", "account_status", "account_group", "business_direction")
    columns = "id,source_row,platform,account_status,account_group,business_direction," + ",".join(_SEARCH_FIELDS)
    matches = []
    for row in connection.execute(f"SELECT {columns} FROM account_directory_rows ORDER BY source_row,id"):
        if any(getattr(payload, field) and row[field] != getattr(payload, field) for field in filters):
            continue
        if query and query not in " ".join(_text(row[field]) for field in _SEARCH_FIELDS).casefold():
            continue
        matches.append(int(row["id"]))
    return matches


def _account_models(connection: sqlite3.Connection, rows: list[dict], *, roster: Mapping,
                    frequencies: Mapping, admissions: Mapping) -> dict[int, dict]:
    account_ids = list(dict.fromkeys(row["account_id"] for row in rows if row["account_id"] is not None))
    if not account_ids:
        return {}
    account_ids_json = json.dumps(account_ids)
    accounts = [dict(row) for row in connection.execute(
        "SELECT * FROM accounts WHERE id IN (SELECT value FROM json_each(?))", (account_ids_json,))]
    sql = (ACCOUNT_IDENTITY_STATS_SQL
           .replace("SELECT api.id,", "SELECT api.account_id, api.id,")
           .replace("WHERE api.account_id=?", "WHERE api.account_id IN (SELECT value FROM json_each(?))")
           .replace("AND c.platform=api.platform", f"AND c.platform=api.platform AND {canonical_content_predicate(connection)}"))
    identities_by_account: dict[int, list[dict]] = defaultdict(list)
    identity_ids = []
    for row in connection.execute(sql, (account_ids_json,)):
        identity = dict(row)
        account_id = identity.pop("account_id")
        identities_by_account[account_id].append(identity)
        identity_ids.append(identity["id"])
    statistics = select_account_metrics(connection, identity_ids)
    snapshot = None
    if roster.get("ready"):
        source_family = str(roster.get("source_family") or "matrix")
        snapshot_id = roster.get("snapshot_id")
        snapshot = (snapshot_by_id(connection, int(snapshot_id)) if snapshot_id is not None
                    else latest_family_snapshot(connection, source_family))
        if snapshot is not None and snapshot["source_family"] != source_family:
            raise RosterError("roster_evidence_mismatch", "Requested roster snapshot belongs to another source family")
    members = {}
    if snapshot is not None and identity_ids:
        members = {row["account_identity_id"]: dict(row) for row in connection.execute(
            "SELECT * FROM account_roster_members WHERE snapshot_id=? "
            "AND account_identity_id IN (SELECT value FROM json_each(?))",
            (snapshot["id"], json.dumps(identity_ids)),
        )}
    classification = {row["account_id"]: {field: row[field] for field in
                      ("account_group", "business_direction")} for row in rows if row["account_id"] is not None}
    output = {}
    for account in accounts:
        identities = identities_by_account.get(account["id"], [])
        member = members.get(identities[0]["id"]) if identities else None
        metadata = json.loads(member["metadata_json"]) if member else {}
        projected_metadata = {
            "matrix_account_id": member["matrix_account_id"] if member else None,
            "profile_ref": member["profile_ref"] if member else None,
            "monitoring_status": member["monitoring_status"] if member else "unknown",
            "authorization_status": member["authorization_status"] if member else "unknown",
            "nickname": metadata.get("nickname"), "avatar_url": metadata.get("avatar_url"),
            "unique_id": metadata.get("display_account_id"),
        }
        output[account["id"]] = account_read_model(
            connection, account, roster=roster, update_frequencies=frequencies, admission_members=admissions,
            read_projection={"metadata": projected_metadata, "identities": identities,
                             "statistics": statistics, "classification": classification[account["id"]]},
        )
    return output


def _project_directory(connection: sqlite3.Connection, ids: list[int], *, roster: Mapping,
                       frequencies: Mapping, admissions: Mapping) -> list[dict]:
    if not ids:
        return []
    rows = [dict(row) for row in connection.execute(
        "SELECT * FROM account_directory_rows WHERE id IN (SELECT value FROM json_each(?)) ORDER BY source_row,id",
        (json.dumps(ids),),
    )]
    models = _account_models(connection, rows, roster=roster, frequencies=frequencies, admissions=admissions)
    items = directory_account_items(connection, roster=roster, update_frequencies=frequencies,
                                    admission_members=admissions, rows=rows, account_models=models)
    from .account_catalog_capture import annotate_accounts
    from .account_intake import annotate_account_preparation
    annotate_accounts(connection, items, live=False)
    annotate_account_preparation(connection, items)
    return items


def compact_account(item: Mapping[str, Any]) -> dict[str, Any]:
    result = {key: value for key, value in item.items() if key != "account_summary"}
    result["has_account_summary"] = bool(item.get("account_summary"))
    result["platforms"] = [
        {key: value for key, value in identity.items() if key not in {"metric_fields", "statistic_identity_metadata"}}
        for identity in item["platforms"]
    ]
    return result


def search_accounts(payload: Any, *, db_path: Path, read_only: bool = True,
                    compact: bool = False, timings: dict[str, Any] | None = None) -> dict[str, Any]:
    started = monotonic()
    with live_wal_read_only_connections() if read_only else nullcontext():
        with _read_transaction(db_path, read_only=read_only) as connection:
            _timing(timings, "account_connect", started)
            if not has_account_directory(connection):
                # Pre-directory databases keep their established filtering and
                # sorting contract. Current installations take the batched path.
                from .api import _account_search
                result = _account_search(payload, db_path=db_path, read_only=read_only)
            else:
                started = monotonic()
                ids = _matching_directory_ids(connection, payload)
                _timing(timings, "account_select", started)
                offset = (payload.page - 1) * payload.page_size
                started = monotonic()
                roster, frequencies, admissions = _context(connection)
                _timing(timings, "account_context", started)
                started = monotonic()
                items = _project_directory(connection, ids[offset:offset + payload.page_size],
                                           roster=roster, frequencies=frequencies, admissions=admissions)
                _timing(timings, "account_projection", started)
                from .account_intake import has_account_intake
                result = {"items": items, "total": len(ids), "page": payload.page, "page_size": payload.page_size,
                          "account_management_version": 3 if has_account_intake(connection) else 2,
                          "account_directory_version": 2, "roster": roster}
    if compact and result.get("account_directory_version") == 2:
        result["items"] = [compact_account(item) for item in result["items"]]
        result["list_contract_version"] = LIST_CONTRACT_VERSION
    return result


def account_detail(directory_row_id: int, *, db_path: Path, read_only: bool = True,
                   timings: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if type(directory_row_id) is not int or directory_row_id < 1:
        return None
    started = monotonic()
    with live_wal_read_only_connections() if read_only else nullcontext():
        with _read_transaction(db_path, read_only=read_only) as connection:
            _timing(timings, "account_connect", started)
            if not has_account_directory(connection) or not connection.execute(
                "SELECT 1 FROM account_directory_rows WHERE id=?", (directory_row_id,),
            ).fetchone():
                return None
            started = monotonic()
            roster, frequencies, admissions = _context(connection)
            items = _project_directory(connection, [directory_row_id], roster=roster,
                                       frequencies=frequencies, admissions=admissions)
            _timing(timings, "account_detail", started)
            return items[0] if items else None
