"""Explicit, offline schema20 account-cleanup projection.

The source is read-only and its consistent backup remains the historical evidence
authority.  This is not an ordinary migration and does not inherit paid readiness.
Only a newly created isolated database is written; runtime triggers are installed
after the declared projection has been copied and its foreign keys validated.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


CONTRACT = "account-cleanup-projection-v1"
RESET_TABLES = frozenset("""
account_cleanup_budget_daily account_roster_members account_roster_snapshots account_state_events
acquisition_profile_activations activation_cancellations admission_reservations
authorization_issuance_consumptions capture_completion_events
capture_fallback_blueprint_members capture_fallback_blueprints
capture_paid_send_gate_events capture_route_assignments capture_source_plans
capture_watermarks capture_work_items compensation_authorization_decisions
compensation_authorization_issuances compensation_authorizations
data_quality_receipts deployment_readiness_receipts fetch_dead_letters
fetch_request_batch_members fetch_request_batches fetch_request_executions
fetch_request_member_dispositions import_batches import_rows
legacy_paid_scope_exclusions legacy_provider_send_evidences migration_audit
migration_row_audit operational_alerts paid_provider_dispatch_events
pipeline_paid_drain_events profile_day_coverage_receipts
provider_authorization_states provider_billing_evidence_claims provider_budget_batches
provider_circuit_states provider_paid_scope_claims provider_probe_authorization_issuances
provider_probe_authorizations provider_readiness_receipts provider_request_start_events
provider_usage provider_usage_settlement_events provider_usage_settlements
routing_input_changes runtime_receipt_revocations scan_verification_receipts
scheduler_run_attempts scheduler_runs send_boundary_eligibility_receipts
send_boundary_member_eligibility_receipts spu_association_runs
transport_continuity_permit_members transport_continuity_permits
""".split())
PRESERVE_TABLES = frozenset("""
account_directory_rows account_metric_observations account_platform_identities account_provider_references
accounts audience_dim audience_scene_map comment_capture_pages comment_capture_runs
comment_evidence_versions comment_user_scores comments content_aliases
content_audience_links content_availability_observations content_identities
content_identity_merge_events content_items content_metric_corrections
content_metric_field_facts content_metric_observations
content_metric_projection_causal_events content_metric_projection_versions
content_metric_snapshots content_metric_ttl_transition_events content_scene_links
content_spu_links duplicate_calibration_runs duplicate_fingerprints duplicate_relations
evaluation_matches evaluation_releases evaluation_versions evidence_artifacts
evidence_envelopes fetch_attempts fetch_slots fetch_transport_receipts
interaction_user_classification_versions interaction_users llm_judgements
media_processing_slots metric_policy_transitions pending_platform_identities
provider_raw_blobs provider_raw_responses provider_response_field_evidences
raw_archive_members raw_archives raw_retention_events report_files report_revisions
report_tasks scene_dim schema_migrations selling_point_scenes selling_points
spu_alias spu_audience_map spu_catalog task_contents task_events taxonomy_versions
transport_quarantine_members
""".split())
# These facts must survive verbatim for every retained content/account. A source
# dependency pointing outside the retained set is a blocker, never silent loss.
PROTECTED_CONTENT_TABLES = frozenset("""
content_items content_metric_snapshots content_metric_observations
content_metric_field_facts content_availability_observations
content_metric_projection_versions evaluation_versions evidence_artifacts
evidence_envelopes comment_capture_runs comment_evidence_versions comment_user_scores
llm_judgements content_identities content_aliases content_spu_links
content_scene_links content_audience_links duplicate_fingerprints media_processing_slots
""".split())
TRANSFORMED_COLUMNS = {"provider_raw_responses": {
    "account_id": "CASE WHEN s.account_id IN (SELECT rid FROM keep_accounts) THEN s.account_id ELSE NULL END",
    "content_id": "CASE WHEN s.content_id IN (SELECT rid FROM keep_content_items) THEN s.content_id ELSE NULL END",
    "fetch_attempt_id": "CASE WHEN s.fetch_attempt_id IN (SELECT rid FROM keep_fetch_attempts) THEN s.fetch_attempt_id ELSE NULL END",
    "transport_receipt_id": "CASE WHEN s.transport_receipt_id IN (SELECT rid FROM keep_fetch_transport_receipts) THEN s.transport_receipt_id ELSE NULL END",
}}


class CleanupError(RuntimeError):
    """The requested offline projection could not preserve its contract."""


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _file_hash(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def _read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10)
    connection.execute("PRAGMA query_only=ON")
    return connection


def _table_info(connection: sqlite3.Connection, table: str) -> list[tuple[Any, ...]]:
    return list(connection.execute(f"PRAGMA source.table_info({_q(table)})"))


def _keep(table: str) -> str:
    return _q("keep_" + table)


def _fk_groups(connection: sqlite3.Connection, table: str) -> list[list[tuple[Any, ...]]]:
    groups: dict[int, list[tuple[Any, ...]]] = {}
    for row in connection.execute(f"PRAGMA source.foreign_key_list({_q(table)})"):
        groups.setdefault(row[0], []).append(row)
    return list(groups.values())


def _prune_foreign_keys(connection: sqlite3.Connection, tables: set[str]) -> int:
    """Restrict child projections until every copied non-null FK closes."""
    removed = 0
    for table in sorted(tables):
        if table in RESET_TABLES:
            continue
        for group in _fk_groups(connection, table):
            parent = group[0][2]
            if any(row[3] in TRANSFORMED_COLUMNS.get(table, {}) for row in group):
                continue
            populated = " AND ".join(f"c.{_q(row[3])} IS NOT NULL" for row in group)
            matches = " AND ".join(f"p.{_q(row[4])}=c.{_q(row[3])}" for row in group)
            sql = (f"DELETE FROM {_keep(table)} WHERE rid IN (SELECT c.rowid FROM source.{_q(table)} c "
                   f"JOIN {_keep(table)} k ON k.rid=c.rowid WHERE {populated} AND NOT EXISTS "
                   f"(SELECT 1 FROM source.{_q(parent)} p JOIN {_keep(parent)} pk ON pk.rid=p.rowid WHERE {matches}))")
            removed += connection.execute(sql).rowcount
    return removed


def _prune_unreferenced(connection: sqlite3.Connection, parent: str,
                        references: list[tuple[str, str]]) -> None:
    referenced = _q("referenced_" + parent)
    connection.execute(f"CREATE TEMP TABLE {referenced}(id INTEGER PRIMARY KEY)")
    for child, column in references:
        connection.execute(f"INSERT OR IGNORE INTO {referenced} SELECT c.{_q(column)} "
                           f"FROM source.{_q(child)} c JOIN {_keep(child)} k ON k.rid=c.rowid "
                           f"WHERE c.{_q(column)} IS NOT NULL")
    connection.execute(f"DELETE FROM {_keep(parent)} WHERE rid IN (SELECT p.rowid FROM source.{_q(parent)} p "
                       f"WHERE p.id NOT IN (SELECT id FROM {referenced}))")


def _digest_rows(connection: sqlite3.Connection, sql: str) -> dict[str, Any]:
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(sql):
        # SQLite blobs are encoded explicitly; no lossy coercion of business data.
        value = [{"blob_hex": item.hex()} if isinstance(item, bytes) else item for item in row]
        digest.update(_canonical(value).encode("utf-8") + b"\n")
        count += 1
    return {"rows": count, "sha256": digest.hexdigest()}


def _approved_sets(plan: Mapping[str, Any]) -> tuple[set[int], set[int]]:
    groups = plan.get("subject_id_sets")
    if not isinstance(groups, Mapping):
        raise CleanupError("approved plan must contain subject_id_sets")
    deleted = {int(value) for value in groups["other_subjects"]}
    retained = {int(value) for key in ("exact_uid_confirmed", "unresolved_phone_candidates", "additional_new_uid_phone_candidates")
                for value in groups[key]}
    if not deleted or deleted & retained:
        raise CleanupError("approved deletion and preservation sets overlap or are empty")
    return deleted, retained


def _prepare_selection(connection: sqlite3.Connection, tables: set[str],
                       deleted: set[int], retained: set[int], expected_deleted_contents: int) -> dict[str, Any]:
    for table in sorted(tables):
        connection.execute(f"CREATE TEMP TABLE {_keep(table)} (rid INTEGER PRIMARY KEY)")
        if table not in RESET_TABLES:
            connection.execute(f"INSERT INTO {_keep(table)} SELECT rowid FROM source.{_q(table)}")
    connection.execute("CREATE TEMP TABLE removed_accounts(id INTEGER PRIMARY KEY)")
    connection.executemany("INSERT INTO removed_accounts VALUES (?)", [(value,) for value in sorted(deleted)])
    actual = {row[0] for row in connection.execute("SELECT id FROM source.accounts")}
    if actual != deleted | retained:
        raise CleanupError("source account set differs from approved exact partition")
    connection.execute("CREATE TEMP TABLE removed_contents(id INTEGER PRIMARY KEY)")
    connection.execute("INSERT INTO removed_contents SELECT id FROM source.content_items WHERE account_id IN (SELECT id FROM removed_accounts)")
    count = connection.execute("SELECT COUNT(*) FROM removed_contents").fetchone()[0]
    if count != expected_deleted_contents:
        raise CleanupError(f"deleted content count changed: expected {expected_deleted_contents}, found {count}")
    connection.execute("DELETE FROM keep_accounts WHERE rid IN (SELECT id FROM removed_accounts)")
    connection.execute("DELETE FROM keep_content_items WHERE rid IN (SELECT id FROM removed_contents)")
    # Remove an entire historical report when any of its frozen cohort is removed.
    removed_reports = [row[0] for row in connection.execute("SELECT DISTINCT task_id FROM source.task_contents WHERE content_id IN (SELECT id FROM removed_contents)")]
    connection.execute("DELETE FROM keep_report_tasks WHERE rid IN (SELECT rowid FROM source.report_tasks WHERE id IN "
                       "(SELECT task_id FROM source.task_contents WHERE content_id IN (SELECT id FROM removed_contents)))")
    # Prevent a legacy pending identity from reintroducing an explicitly removed UID.
    connection.execute("DELETE FROM keep_pending_platform_identities WHERE rid IN (SELECT p.rowid FROM source.pending_platform_identities p "
                       "JOIN source.account_platform_identities i ON i.platform=p.platform AND i.uid=p.uid "
                       "WHERE i.account_id IN (SELECT id FROM removed_accounts))")
    rounds = 0
    while _prune_foreign_keys(connection, tables):
        rounds += 1
        if rounds > len(tables):
            raise CleanupError("foreign-key projection did not converge")
    # Retain shared raw evidence only when a surviving business row actually uses
    # it. An old roster's raw manifest is not needed by the new runtime roster.
    raw_references = [(table, row[3]) for table in sorted(tables - RESET_TABLES)
                      if table not in {"raw_retention_events", "raw_archive_members"}
                      for row in connection.execute(f"PRAGMA source.foreign_key_list({_q(table)})")
                      if row[2] == "provider_raw_responses"]
    raw_references.append(("account_provider_references", "source_raw_response_id"))
    _prune_unreferenced(connection, "provider_raw_responses", raw_references)
    _prune_unreferenced(connection, "provider_raw_blobs", [("provider_raw_responses", "raw_blob_id")])
    _prune_unreferenced(connection, "interaction_users", [("comments", "interaction_user_id")])
    while _prune_foreign_keys(connection, tables):
        rounds += 1
        if rounds > 2 * len(tables):
            raise CleanupError("raw-evidence projection did not converge")
    # No valid business fact may disappear as a side effect of dependency cleanup.
    protected: dict[str, int] = {}
    for table in sorted(PROTECTED_CONTENT_TABLES & tables):
        column = "id" if table == "content_items" else "content_id"
        expected = connection.execute(f"SELECT COUNT(*) FROM source.{_q(table)} WHERE {_q(column)} NOT IN (SELECT id FROM removed_contents)").fetchone()[0]
        actual_count = connection.execute(f"SELECT COUNT(*) FROM {_keep(table)}").fetchone()[0]
        if expected != actual_count:
            raise CleanupError(f"retained business facts lose a dependency: {table}: {expected} != {actual_count}")
        protected[table] = expected
    # Comments and matching evaluations inherit their retained parent membership.
    for table, parent, column in (("comments", "comment_evidence_versions", "evidence_version_id"),
                                   ("evaluation_matches", "evaluation_versions", "evaluation_id")):
        expected = connection.execute(f"SELECT COUNT(*) FROM source.{_q(table)} c JOIN source.{_q(parent)} p ON p.id=c.{_q(column)} "
                                      "WHERE p.content_id NOT IN (SELECT id FROM removed_contents)").fetchone()[0]
        actual_count = connection.execute(f"SELECT COUNT(*) FROM {_keep(table)}").fetchone()[0]
        if expected != actual_count:
            raise CleanupError(f"retained dependent facts changed: {table}")
        protected[table] = expected
    return {"removed_account_ids": sorted(deleted), "retained_account_ids": sorted(retained),
            "removed_content_ids": [row[0] for row in connection.execute("SELECT id FROM removed_contents ORDER BY id")],
            "removed_report_task_ids": sorted(removed_reports), "protected_counts": protected, "fk_projection_rounds": rounds}


BUDGET_TABLE = "account_cleanup_budget_daily"
BUDGET_CARRY_SCHEMA = """
CREATE TABLE IF NOT EXISTS account_cleanup_budget_daily (
    business_day TEXT PRIMARY KEY CHECK(length(business_day)=10),
    source_snapshot_sha256 TEXT NOT NULL CHECK(length(source_snapshot_sha256)=64),
    summary_json TEXT NOT NULL CHECK(json_valid(summary_json)),
    summary_sha256 TEXT NOT NULL CHECK(length(summary_sha256)=64)
);
CREATE TRIGGER IF NOT EXISTS trg_cleanup_budget_no_update BEFORE UPDATE ON account_cleanup_budget_daily
BEGIN SELECT RAISE(ABORT,'cleanup budget baseline is immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_cleanup_budget_no_delete BEFORE DELETE ON account_cleanup_budget_daily
BEGIN SELECT RAISE(ABORT,'cleanup budget baseline is immutable'); END;
"""


def _budget_rows(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (BUDGET_TABLE,)).fetchone():
        return {}
    result = {}
    for day, source_hash, encoded, digest in connection.execute(
            "SELECT business_day,source_snapshot_sha256,summary_json,summary_sha256 FROM account_cleanup_budget_daily"):
        value = json.loads(encoded)
        sealed = {"business_day": day, "source_snapshot_sha256": source_hash, "summary": value}
        if hashlib.sha256(_canonical(sealed).encode()).hexdigest() != digest:
            raise CleanupError("cleanup budget baseline digest differs")
        result[day] = value
    return result


def budget_carry(connection: sqlite3.Connection, day: str) -> dict[str, Any]:
    """Read compact, immutable historical expenses; current usage is added by its ledger."""
    rows = _budget_rows(connection)
    value = dict(rows.get(day, {}))
    value["lifetime_unknown_count"] = sum(item["unknown_count"] for item in rows.values())
    value["lifetime_unknown_amount"] = sum(item["unknown_amount"] for item in rows.values())
    value["lifetime_unverified_count"] = sum(item["unverified_count"] for item in rows.values())
    value["lifetime_unverified_amount"] = sum(item["unverified_amount"] for item in rows.values())
    return value


def archived_slot_holds(connection: sqlite3.Connection) -> dict[int, dict[str, int]]:
    result: dict[int, dict[str, int]] = {}
    for value in _budget_rows(connection).values():
        for slot_id, states in value.get("slot_holds", {}).items():
            target = result.setdefault(int(slot_id), {})
            for state, count in states.items():
                target[state] = target.get(state, 0) + count
    return result


def archived_pending_category(connection: sqlite3.Connection, category: str) -> bool:
    return any(value.get("pending_categories", {}).get(category, 0) for value in _budget_rows(connection).values())


def install_budget_carry(connection: sqlite3.Connection, source: sqlite3.Connection,
                         *, source_hash: str) -> dict[str, Any]:
    """Aggregate the exact budget reader contract, keeping unresolved slot holds.

    Historic billing remains unresolved until reconciled against the offline
    source. This baseline is never treated as a supplier payment confirmation.
    """
    from .provider_budget import budget_day, budget_summary

    source.row_factory = sqlite3.Row
    prior = _budget_rows(source)
    rows = list(source.execute("SELECT * FROM provider_usage WHERE lower(provider)='tikhub' AND currency='USD'"))
    by_day: dict[str, list[tuple[sqlite3.Row, dict[str, Any]]]] = {day: [] for day in prior}
    for row in rows:
        details = json.loads(row["details_json"] or "{}")
        day = str(details.get("budget_day") or budget_day(str(row["recorded_at"])))
        by_day.setdefault(day, []).append((row, details))
    retained_slots = {row[0] for row in connection.execute("SELECT id FROM fetch_slots")}
    # Execute separately: executescript would implicitly commit the caller's projection.
    for sql in BUDGET_CARRY_SCHEMA.split(";\n"):
        if sql.strip():
            connection.execute(sql)
    receipts = []
    for day, usages in sorted(by_day.items()):
        summary = budget_summary(source, at=day + "T12:00:00+08:00")
        value = {"total": summary["total_microusd"], "pending": summary["pending_microusd"],
                 "unclassified": summary["legacy_unclassified_microusd"],
                 "categories": summary["categories_microusd"], "buckets": summary["buckets_microusd"],
                 "unknown_count": summary["billing_unknown"]["budget_day_count"],
                 "unknown_amount": summary["billing_unknown"]["budget_day_microusd"],
                 "unverified_count": summary["charged_unverified"]["budget_day_count"],
                 "unverified_amount": summary["charged_unverified"]["budget_day_microusd"],
                 "lent": dict(prior.get(day, {}).get("lent", {})),
                 "received": dict(prior.get(day, {}).get("received", {})),
                 "pending_categories": dict(prior.get(day, {}).get("pending_categories", {})),
                 "slot_holds": {k: dict(v) for k, v in prior.get(day, {}).get("slot_holds", {}).items()
                                if int(k) in retained_slots}}
        for row, details in usages:
            category, state = details.get("category"), details.get("state")
            if state in {"reserved", "sent", "billing_unknown"} and category:
                value["pending_categories"][category] = value["pending_categories"].get(category, 0) + 1
            slot_id = details.get("slot_id")
            if type(slot_id) is int and slot_id in retained_slots and state in {"billing_unknown", "charged_unverified"}:
                states = value["slot_holds"].setdefault(str(slot_id), {})
                states[state] = states.get(state, 0) + 1
            # Match the original _borrowing rule exactly: rows without an
            # explicit budget_day or with zero amount never carry a loan.
            if row["amount"] and details.get("budget_day") == day:
                for lender, amount in details.get("borrowed_from", {}).items():
                    if lender not in summary["categories_microusd"] or type(amount) is not int or amount < 0 or category not in summary["categories_microusd"]:
                        raise CleanupError("corrupt historical budget lending allocation")
                    value["lent"][lender] = value["lent"].get(lender, 0) + amount
                    value["received"][category] = value["received"].get(category, 0) + amount
        sealed = {"business_day": day, "source_snapshot_sha256": source_hash, "summary": value}
        digest = hashlib.sha256(_canonical(sealed).encode()).hexdigest()
        connection.execute("INSERT INTO account_cleanup_budget_daily VALUES(?,?,?,?)",
                           (day, source_hash, _canonical(value), digest))
        receipts.append({"business_day": day, "total_microusd": value["total"], "summary_sha256": digest})
    return {"table": BUDGET_TABLE, "days": receipts, "source_usage_rows": len(rows),
            "unresolved_slot_count": len(archived_slot_holds(connection)),
            "billing_resolution": "archived_unresolved_requires_offline_source_reconciliation"}


def build_candidate(*, source_database: Path, output_directory: Path,
                    approved_plan: Mapping[str, Any], expected_deleted_contents: int,
                    account_payload: Mapping[str, Any] | None = None,
                    expected_source_sha256: str | None = None) -> dict[str, Any]:
    """Materialize one new, isolated, paid-HOLD candidate plus a sealed source backup."""
    source_database = source_database.resolve(strict=True)
    output_directory = output_directory.absolute()
    prepared_backup = output_directory.exists() and source_database == (output_directory / "source.sqlite3").resolve()
    if output_directory.exists() and not prepared_backup:
        raise CleanupError("output directory must be new or contain the explicitly sealed source backup")
    if prepared_backup and (expected_source_sha256 is None or any((output_directory / name).exists() for name in ("candidate.sqlite3", "receipt.json"))):
        raise CleanupError("prepared backup requires its expected hash and new candidate/receipt paths")
    if not prepared_backup and (output_directory == source_database.parent or source_database.is_relative_to(output_directory)):
        raise CleanupError("output must be isolated from the source database")
    deleted, retained = _approved_sets(approved_plan)
    output_directory.mkdir(parents=True, mode=0o700, exist_ok=prepared_backup)
    backup_path = output_directory / "source.sqlite3"
    candidate_path = output_directory / "candidate.sqlite3"
    timestamp = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    with _read_only(source_database) as source:
        source.execute("BEGIN")
        if source.execute("PRAGMA user_version").fetchone()[0] != 20:
            raise CleanupError("cleanup requires schema20 source")
        if prepared_backup:
            if any(Path(str(backup_path) + suffix).exists() for suffix in ("-wal", "-shm", "-journal")):
                raise CleanupError("prepared source backup must have no SQLite sidecars")
        else:
            with sqlite3.connect(backup_path) as backup:
                source.backup(backup)
        source.rollback()
    source_hash = _file_hash(backup_path)
    if expected_source_sha256 is not None and source_hash != expected_source_sha256:
        raise CleanupError("source snapshot hash differs from the approved backup")
    connection = sqlite3.connect(candidate_path, uri=True)
    candidate_path.chmod(0o600)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("ATTACH DATABASE ? AS source", (backup_path.as_uri() + "?mode=ro&immutable=1",))
        objects = list(connection.execute("SELECT type,name,tbl_name,sql FROM source.sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY type,name"))
        tables = {row[1] for row in objects if row[0] == "table"}
        unknown = tables - RESET_TABLES - PRESERVE_TABLES
        if unknown:
            raise CleanupError("unclassified source tables: " + ", ".join(sorted(unknown)))
        connection.execute("BEGIN")
        connection.execute("PRAGMA defer_foreign_keys=ON")
        selection = _prepare_selection(connection, tables, deleted, retained, expected_deleted_contents)
        # Create schema in the empty destination, not by relaxing source/runtime
        # protections. The declared one-time projection owns insertion only.
        for kind, _, _, sql in objects:
            if kind == "table":
                connection.execute(sql)
        # Existing FK lookup indexes must precede parent insertion: deferred
        # constraints still inspect already copied children for each parent row.
        for kind, _, _, sql in objects:
            if kind == "index":
                connection.execute(sql)
        # Some runtime FKs lack a child-side index because ordinary operation
        # rarely inserts an old parent. A bulk rebuild must index these temporary
        # reverse lookups as well, while keeping all FK checks enabled.
        construction_indexes = []
        for table in sorted(tables - RESET_TABLES):
            for group in _fk_groups(connection, table):
                name = f"cleanup_copy_fk_{table}_{group[0][0]}"
                connection.execute(f"CREATE INDEX {_q(name)} ON {_q(table)} ({','.join(_q(row[3]) for row in group)})")
                construction_indexes.append(name)
        manifests: dict[str, Any] = {}
        detached = [dict(zip(("raw_response_id", "archived_fetch_attempt_id", "archived_transport_receipt_id", "archived_account_id", "archived_content_id"), row)) for row in connection.execute(
            "SELECT r.id,CASE WHEN r.fetch_attempt_id NOT IN (SELECT rid FROM keep_fetch_attempts) THEN r.fetch_attempt_id END,"
            "CASE WHEN r.transport_receipt_id NOT IN (SELECT rid FROM keep_fetch_transport_receipts) THEN r.transport_receipt_id END,"
            "CASE WHEN r.account_id NOT IN (SELECT rid FROM keep_accounts) THEN r.account_id END,"
            "CASE WHEN r.content_id NOT IN (SELECT rid FROM keep_content_items) THEN r.content_id END "
            "FROM source.provider_raw_responses r JOIN keep_provider_raw_responses k ON k.rid=r.rowid "
            "WHERE r.fetch_attempt_id NOT IN (SELECT rid FROM keep_fetch_attempts) "
            "OR r.transport_receipt_id NOT IN (SELECT rid FROM keep_fetch_transport_receipts) "
            "OR r.account_id NOT IN (SELECT rid FROM keep_accounts) OR r.content_id NOT IN (SELECT rid FROM keep_content_items) ORDER BY r.id")]
        # SQLite evaluates NULL NOT IN an empty set as true; do not record a
        # detachment when every archived reference was already null.
        detached = [row for row in detached if any(value is not None for key, value in row.items() if key != "raw_response_id")]
        for table in sorted(tables):
            columns = [row[1] for row in _table_info(connection, table)]
            projection = ",".join(TRANSFORMED_COLUMNS.get(table, {}).get(column, "s." + _q(column)) for column in columns)
            source_sql = f"SELECT {projection} FROM source.{_q(table)} s JOIN {_keep(table)} k ON k.rid=s.rowid ORDER BY s.rowid"
            connection.execute(f"INSERT INTO {_q(table)} ({','.join(map(_q, columns))}) " + source_sql)
            expected = _digest_rows(connection, source_sql)
            actual = _digest_rows(connection, f"SELECT {','.join(map(_q, columns))} FROM main.{_q(table)} ORDER BY rowid")
            if expected != actual:
                raise CleanupError("projection digest differs: " + table)
            original = connection.execute(f"SELECT COUNT(*) FROM source.{_q(table)}").fetchone()[0]
            manifests[table] = {"source_rows": original, "retained_projection": actual, "removed_rows": original - actual["rows"]}
        # Never reuse a historical numeric ID after removal; copied identities and
        # links remain stable and newly inserted directory identities start later.
        connection.execute("DELETE FROM main.sqlite_sequence")
        connection.execute("INSERT INTO main.sqlite_sequence SELECT * FROM source.sqlite_sequence")
        scopes = {row[0] for table in ("provider_paid_scope_claims", "provider_usage_settlements", "legacy_provider_send_evidences", "legacy_paid_scope_exclusions")
                  for row in connection.execute(f"SELECT DISTINCT scope_identity FROM source.{_q(table)} WHERE scope_identity IS NOT NULL")}
        scopes.update(row[0] for row in connection.execute("SELECT DISTINCT json_extract(details_json,'$.paid_scope_identity') FROM source.provider_usage WHERE json_valid(details_json) AND json_extract(details_json,'$.paid_scope_identity') IS NOT NULL"))
        for scope in sorted(scopes):
            if len(scope) != 64 or any(character not in "0123456789abcdef" for character in scope):
                raise CleanupError("historical paid scope is not a canonical SHA256")
            evidence = {"contract": CONTRACT, "source_snapshot_sha256": source_hash, "scope_identity": scope}
            connection.execute("INSERT INTO legacy_paid_scope_exclusions(scope_identity,evidence_sha256,evidence_ref,reason,created_at) VALUES(?,?,?,?,?)",
                               (scope, hashlib.sha256(_canonical(evidence).encode()).hexdigest(),
                                "cleanup-source-sha256:" + source_hash,
                                "Archived historical paid scope; ordinary replay remains prohibited", timestamp))
        with _read_only(backup_path) as budget_source:
            budget_result = install_budget_carry(connection, budget_source, source_hash=source_hash)
        for name in construction_indexes:
            connection.execute(f"DROP INDEX {_q(name)}")
        # Reinstate every original index/trigger/view before any normal mutation.
        for kind in ("view", "trigger"):
            for obj_kind, _, _, sql in objects:
                if obj_kind == kind:
                    connection.execute(sql)
        connection.execute("PRAGMA user_version=20")
        directory_result = None
        if account_payload is not None:
            from .account_directory import import_account_directory
            directory_result = import_account_directory(connection, account_payload, imported_at=timestamp)
        violations = list(connection.execute("PRAGMA foreign_key_check"))
        if violations:
            raise CleanupError("candidate foreign-key violations: " + repr(violations[:10]))
        integrity = [row[0] for row in connection.execute("PRAGMA integrity_check")]
        if integrity != ["ok"]:
            raise CleanupError("candidate integrity failed: " + repr(integrity[:10]))
        if connection.execute("SELECT COUNT(*) FROM acquisition_profile_activations").fetchone()[0] != 0:
            raise CleanupError("cleanup candidate must not inherit a live activation")
        if connection.execute("SELECT COUNT(*) FROM accounts WHERE id IN (SELECT id FROM removed_accounts)").fetchone()[0]:
            raise CleanupError("removed accounts survived projection")
        if connection.execute("SELECT COUNT(*) FROM content_items WHERE id IN (SELECT id FROM removed_contents)").fetchone()[0]:
            raise CleanupError("removed contents survived projection")
        final_counts = {table: connection.execute(f"SELECT COUNT(*) FROM {_q(table)}").fetchone()[0] for table in sorted(tables | {BUDGET_TABLE})}
        connection.commit()
        connection.execute("DETACH DATABASE source")
        connection.execute("VACUUM")
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    candidate_hash = _file_hash(candidate_path)
    receipt: dict[str, Any] = {
        "contract": CONTRACT, "status": "candidate_verified", "capture_state": "hold_requires_new_installation_and_activation",
        "created_at": timestamp, "source_database": str(source_database),
        "source_backup": {"path": str(backup_path), "sha256": source_hash, "bytes": backup_path.stat().st_size},
        "candidate": {"path": str(candidate_path), "sha256": candidate_hash, "bytes": candidate_path.stat().st_size},
        "approved_plan_sha256": hashlib.sha256(_canonical(approved_plan).encode()).hexdigest(),
        "source_attachment_sha256": approved_plan.get("source_sha256"),
        "selection": selection, "table_projections": manifests, "final_counts": final_counts,
        "budget_carry": budget_result,
        "historical_raw_detachments": detached, "historical_paid_scope_exclusions": len(scopes),
        "declared_projection_columns": {table: sorted(columns) for table, columns in TRANSFORMED_COLUMNS.items()},
        "reset_control_tables": sorted(RESET_TABLES & tables), "directory": directory_result,
        "verification": {"foreign_key_check": "ok", "integrity_check": "ok", "projected_values_sha256_verified": True},
        "limitations": ["Old batch execution and old control chains are no longer replayable in the candidate",
                        "The consistent source backup retains original immutable chains and mixed reports",
                        "No media files were changed; a separate ownership manifest must authorize file cleanup",
                        "No installed-runtime or paid-capture authorization is inherited"],
    }
    receipt["receipt_sha256"] = hashlib.sha256(_canonical(receipt).encode()).hexdigest()
    (output_directory / "receipt.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")
    return receipt
