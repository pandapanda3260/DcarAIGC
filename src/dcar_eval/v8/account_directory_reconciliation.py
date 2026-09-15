"""Offline, idempotent reconciliation of the directory into account intake.

Every planner tick checks historical rows, including rows that have no usable
locator. The intake journal records that technical outcome once per locator
revision; ordinary preparation owns all network, billing and activation gates.
Directory source data and manually maintained fields are never rewritten here.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Mapping

CONTRACT = "account-directory-reconciliation-v1"
LINK_FIELDS = ("profile_url", "profile_ref", "主页链接", "账号主页链接")


def directory_locator_snapshot(row: Mapping[str, Any]) -> dict[str, Any]:
    """Only typed locator fields; names, phones and free-form notes are not IDs."""
    if row.get("locator_revision", 0):
        current = json.loads(row.get("locator_json") or "{}")
        if not isinstance(current, dict):
            raise ValueError("directory_locator_not_object")
        # UID/display columns remain searchable projections. A divergent write
        # invalidates pending authority instead of silently choosing one value.
        return {"platform": row["platform"], "uid": row.get("uid") or "",
                "display_account_id": row.get("display_account_id") or "",
                "profile_links": [current["profile_url"]] if current.get("profile_url") else [],
                "references": current.get("references", {}),
                "locator_revision": row["locator_revision"], "locator": current}
    snapshot = {key: row.get(key) or "" for key in ("platform", "uid", "display_account_id")}
    raw = json.loads(row["raw_json"])
    if not isinstance(raw, dict):
        return {**snapshot, "source_error": "directory_source_not_object", "profile_links": []}
    containers = [raw]
    summary = raw.get("account_summary", {})
    if isinstance(summary, dict):
        metadata = summary.get("metadata", {})
        if isinstance(metadata, dict) and isinstance(metadata.get("enrichment_profile"), dict):
            containers.append(metadata["enrichment_profile"])
    snapshot["profile_links"] = sorted({item[key].strip() for item in containers for key in LINK_FIELDS
        if isinstance(item.get(key), str) and item[key].strip()})
    return snapshot


def directory_value(row: Mapping[str, Any]) -> dict[str, Any]:
    snapshot = directory_locator_snapshot(row)
    if snapshot.get("source_error"):
        raise ValueError(snapshot["source_error"])
    value = {key: snapshot[key] for key in ("platform", "uid", "display_account_id")}
    if "locator" in snapshot:
        stored = snapshot["locator"]
        if any((stored.get(key) or "") != value[key] for key in value):
            raise ValueError("directory_locator_projection_changed")
        return {**value, "profile_url": stored.get("profile_url", ""),
                "references": stored.get("references", {})}
    links = snapshot["profile_links"]
    if len(links) > 1 and not value["uid"]:
        raise ValueError("multiple_source_profile_links")
    if not value["uid"] and links:
        value["profile_url"] = links[0]
    return value


def validate_request_directory(connection: sqlite3.Connection, request: Mapping[str, Any]) -> None:
    """Reject a stale in-flight result even when a changed locator kept its UID."""
    from .account_intake import _fingerprint
    result = json.loads(request["result_json"])
    expected = result.get("directory_locator_sha256")
    if not expected:
        return  # Historical requests retain their existing explicit ID checks.
    row = connection.execute("SELECT * FROM account_directory_rows WHERE id=?", (request["directory_row_id"],)).fetchone()
    if row is None or _fingerprint(directory_locator_snapshot(dict(row))) != expected:
        raise ValueError("preparation_input_changed")
    if dict(row).get("locator_revision", 0):
        directory_value(dict(row))  # validates typed locator/projection equality


def _source(directory: Mapping[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    from .account_intake import _fingerprint
    return {"kind": "directory_backfill", "reconciliation_contract": CONTRACT,
        "directory_row_id": directory["id"], "source_sha256": directory["source_sha256"],
        "name": directory["source_name"], "sheet": directory["source_sheet"], "row": directory["source_row"],
        "directory_locator_sha256": _fingerprint(snapshot)}


def _matches_legacy_request(request: Mapping[str, Any], directory: Mapping[str, Any], value: dict[str, Any] | None) -> bool:
    """Adopt pre-reconciliation journals without repurchasing existing requests."""
    from .account_intake import preparation_key
    if request["platform"] != directory["platform"] or request["account_id"] not in (None, directory["account_id"]):
        return False
    if value is None:
        return False
    prior = json.loads(request["input_json"])
    if preparation_key(value) == request["preparation_key"]:
        return True
    # A backend request can retain a supplied URL in its input journal while
    # its directory already has the canonical UID. UID remains the first key.
    if value.get("uid") and prior.get("uid") == value["uid"]:
        return True
    return (not value.get("uid") and not prior.get("uid") and bool(value.get("display_account_id"))
        and value["display_account_id"] == prior.get("display_account_id")
        and (not value.get("profile_url") or value["profile_url"] == prior.get("profile_url")))


def _block(connection: sqlite3.Connection, *, key: str, directory: Mapping[str, Any], snapshot: dict[str, Any],
           value: dict[str, Any] | None, reason: str, classification: str, at: str) -> dict[str, Any]:
    from .account_intake import _fingerprint, _intake_result, _json, preparation_key
    input_value = value if value is not None else {key: snapshot[key] for key in ("platform", "uid", "display_account_id")}
    result = {"status": "blocked", "activation_status": "blocked", "reason": reason,
        "preparation_error": reason, "directory_classification": classification,
        "submitted_input_sha256": _fingerprint(input_value), "directory_locator_snapshot": snapshot,
        "directory_locator_sha256": _fingerprint(snapshot)}
    cursor = connection.execute("""INSERT INTO account_intake_requests
        (request_key,input_sha256,preparation_key,platform,input_json,source_json,directory_row_id,
         account_id,result_json,created_at,updated_at,completed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (key, _fingerprint(input_value), preparation_key(input_value), directory["platform"], _json(input_value),
         _json(_source(directory, snapshot)), directory["id"], directory["account_id"], _json(result), at, at, at))
    request = dict(connection.execute("SELECT * FROM account_intake_requests WHERE id=?", (cursor.lastrowid,)).fetchone())
    return _intake_result(connection, request)


def _supersede(connection: sqlite3.Connection, directory_id: int, *, at: str) -> None:
    from .account_intake import _json
    for row in connection.execute("SELECT id,result_json FROM account_intake_requests WHERE directory_row_id=? AND completed_at IS NULL", (directory_id,)).fetchall():
        result = json.loads(row["result_json"])
        result.update(status="superseded", activation_status="blocked", preparation_error="preparation_input_changed")
        connection.execute("UPDATE account_intake_requests SET result_json=?,updated_at=?,completed_at=? WHERE id=?",
            (_json(result), at, at, row["id"]))


def _project_verified_identity(connection: sqlite3.Connection, directory: Mapping[str, Any],
                               member: Mapping[str, Any], *, at: str) -> bool:
    """Materialize this pass's verified raw evidence, never an imported label."""
    if (directory["identity_status"] == "existing_verified"
            or member.get("eligible") is not True
            or member.get("reason_code") != "eligible"
            or member.get("locator_evidence", {}).get("kind") not in {"provider_profile_raw", "prepared_profile_chain"}
            or any(member.get(key) != directory.get(key) for key in ("account_id", "platform", "uid"))
            or member.get("directory_row_id") != directory["id"]
            or member.get("identity_id") != member.get("account_identity_id")):
        return False
    # The caller already read and hashed the source chain in this transaction.
    # Recheck only exact DB identity ownership; do not perform another raw read.
    updated = connection.execute("""UPDATE account_directory_rows
        SET identity_status='existing_verified',updated_at=?
        WHERE id=? AND account_id=? AND platform=? AND uid=? AND identity_status=?
          AND EXISTS (SELECT 1 FROM account_platform_identities i WHERE i.id=?
            AND i.account_id=account_directory_rows.account_id
            AND i.platform=account_directory_rows.platform AND i.uid=account_directory_rows.uid)""",
        (at, directory["id"], member["account_id"], member["platform"], member["uid"],
         directory["identity_status"], member["identity_id"]))
    return updated.rowcount == 1


def reconcile_directory(connection: sqlite3.Connection, *, at: str) -> dict[str, Any]:
    """Check all directory rows inside the caller's transaction, without I/O."""
    from .account_capture_eligibility import derive_capture_eligibility
    from .account_intake import (_fingerprint, _intake_result, _json, directory_intake_outcome, has_account_intake, normalize_intake,
                                submit_account_intake)
    from .platform_adapters import next_profile_request
    if not connection.in_transaction:
        raise ValueError("Directory reconciliation requires the caller transaction")
    if not has_account_intake(connection):
        return {"contract": CONTRACT, "total": 0, "counts": {}, "rows": [], "sql_writes": 0}
    before = connection.total_changes
    ready = {row["directory_row_id"]: row for row in derive_capture_eligibility(connection)["eligible_members"]}
    project_verified = connection.execute("PRAGMA user_version").fetchone()[0] in {23, 24}
    latest = {row["directory_row_id"]: dict(row) for row in connection.execute("""SELECT * FROM account_intake_requests WHERE id IN
        (SELECT MAX(id) FROM account_intake_requests WHERE directory_row_id IS NOT NULL GROUP BY directory_row_id)""")}
    rows = []
    for entry in connection.execute("SELECT * FROM account_directory_rows ORDER BY id").fetchall():
        directory = dict(entry)
        row = {"directory_row_id": directory["id"], "platform": directory["platform"], "account_id": directory["account_id"]}
        rows.append(row)
        if directory["id"] in ready:
            if project_verified and _project_verified_identity(connection, directory, ready[directory["id"]], at=at):
                row["identity_projection_updated"] = True
            row.update(status="eligible", reason="existing_profile_evidence_valid")
            continue
        snapshot = directory_locator_snapshot(directory)
        locator_sha = _fingerprint(snapshot)
        value, failure, failure_classification = None, None, "no_locator"
        try:
            value = normalize_intake(directory_value(directory))
            next_profile_request(value)  # Pure capability/locator check; no provider call.
        except ValueError as error:
            failure = str(getattr(error, "error_code", str(error)))
        if not failure:
            try:
                directory_intake_outcome(connection, directory["id"], value)
            except ValueError as error:
                failure, failure_classification = str(getattr(error, "error_code", str(error))), "conflict"
        prior = latest.get(directory["id"])
        result = json.loads(prior["result_json"]) if prior else {}
        same = prior and (result.get("directory_locator_sha256") == locator_sha or
            not result.get("directory_locator_sha256") and _matches_legacy_request(prior, directory, value))
        if same and result.get("status") in {"blocked", "conflict"}:
            # A fixed adapter or a corrected conflicting peer may make an
            # unchanged local locator actionable. Recheck that fact each tick.
            same = bool(failure and result.get("reason") == failure)
        elif same and failure:
            same = False
        if same and result.get("status") != "ready":
            if not result.get("directory_locator_sha256"):
                result.update(directory_locator_snapshot=snapshot, directory_locator_sha256=locator_sha)
                connection.execute("UPDATE account_intake_requests SET result_json=? WHERE id=?", (_json(result), prior["id"]))
                prior = {**prior, "result_json": _json(result)}
            accepted = _intake_result(connection, prior, replayed=True)
        else:
            # A transition back to an earlier locator is a new revision, while
            # unchanged rows replay their current revision without SQL writes.
            key = "directory:" + str(directory["id"]) + ":" + _fingerprint({"locator": snapshot, "previous_intake_id": prior["id"] if prior else None})
            if prior:
                _supersede(connection, directory["id"], at=at)
            if failure:
                accepted = _block(connection, key=key, directory=directory, snapshot=snapshot, value=value,
                    reason=failure, classification=failure_classification, at=at)
            else:
                try:
                    accepted = submit_account_intake(connection, request_key=key, value=value,
                        source=_source(directory, snapshot), at=at)
                except ValueError as error:
                    accepted = _block(connection, key=key, directory=directory, snapshot=snapshot, value=value,
                        reason=str(getattr(error, "error_code", str(error))), classification="conflict", at=at)
            if accepted.get("directory_row_id") not in (None, directory["id"]):
                raise ValueError("directory_backfill_changed_target")
        status = accepted.get("directory_classification") or accepted["status"]
        row.update(status="eligible" if status == "ready" else status, intake_id=accepted["intake_id"],
            replayed=accepted["replayed"], reason=accepted.get("preparation_error") or accepted.get("reason"))
    counts = {key: sum(row["status"] == key for row in rows) for key in sorted({row["status"] for row in rows})}
    return {"contract": CONTRACT, "total": len(rows), "counts": counts, "rows": rows, "sql_writes": connection.total_changes - before}
