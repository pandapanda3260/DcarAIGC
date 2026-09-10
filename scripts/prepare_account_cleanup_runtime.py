#!/usr/bin/env python3
"""Prepare a new cleanup-generation roster and routes on an offline candidate.

No provider request, paid gate, readiness receipt, or qualification is issued.
The source-authority exporter owns the final admitted identity set.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/dcar_eval"))

from v8 import capture_authorizations as auth
from v8 import capture_planning, paid_drain
from v8.account_states import set_account_enabled_in_transaction
from v8.account_cleanup_runtime import validate_source
from v8.profile_activations import INTEGRATED_PROFILE, activation_at, append_activation
from v8.providers import _valid_douyin_sec_user_id
from v8.source_routing import parse_time
from v8.system_roster import seal_system_members
from zoneinfo import ZoneInfo


CONTRACT = "account-cleanup-generation-v1"
OPERATIONS = frozenset({"douyin_user_posts", "douyin_video_detail", "douyin_video_statistics", "douyin_video_comments"})
IDENTITY_FIELDS = ("account_identity_id", "account_id", "platform", "uid")
ACTIVE_FIELDS = ("activation_id", "profile_id", "activation_sha256", "roster_snapshot_id", "roster_members_sha256")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prepare_runtime(
    source: sqlite3.Connection, candidate: sqlite3.Connection, authority: Mapping[str, Any], *,
    generation_id: str, build_receipt_sha256: str, runtime_root_receipt_sha256: str,
    prepared_at: str, effective_at: str, raw_root: Path,
) -> dict[str, Any]:
    if not candidate.in_transaction:
        raise ValueError("Candidate preparation requires the caller's transaction")
    if not generation_id or not all(re.fullmatch(r"[0-9a-f]{64}", value) for value in
                                    (build_receipt_sha256, runtime_root_receipt_sha256)):
        raise ValueError("Generation and sealed build/runtime hashes are required")
    if parse_time(prepared_at) >= parse_time(effective_at):
        raise ValueError("The new activation must be prepared before its effective instant")
    validate_source(authority, at=prepared_at)
    if set(authority["operations"]) != OPERATIONS:
        raise ValueError("Source authority operations differ from the approved cleanup scope")
    for table in ("acquisition_profile_activations", "pipeline_paid_drain_events", "capture_route_assignments"):
        if candidate.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None:
            raise ValueError(f"Candidate control generation is not empty: {table}")
    original = activation_at(source, prepared_at)
    if original is None or original["profile_id"] != INTEGRATED_PROFILE:
        raise ValueError("Source requires its validated integrated activation")
    if {key: original[key] for key in ACTIVE_FIELDS} != dict(authority["source_active"]):
        raise ValueError("Source authority active binding differs from the source database")
    supplied = authority.get("eligible_members")
    if not isinstance(supplied, list) or not supplied:
        raise ValueError("Source authority has no eligible_members")
    selection = sorted(({key: row[key] for key in IDENTITY_FIELDS} for row in supplied),
                       key=lambda row: int(row["account_identity_id"]))
    selection_sha = auth.digest(selection)
    if authority.get("selection_sha256") != selection_sha:
        raise ValueError("Source authority selection SHA does not match its members")
    if len({row["account_identity_id"] for row in selection}) != len(selection):
        raise ValueError("Source authority repeats an identity")
    selected_ids = {row["account_identity_id"] for row in selection}
    pending = []
    for row in candidate.execute(
        "SELECT d.source_row,d.account_id,d.platform,d.uid,i.id account_identity_id FROM account_directory_rows d "
        "JOIN account_platform_identities i ON i.account_id=d.account_id WHERE d.identity_status='existing_verified' "
        "AND d.account_status IN ('daily','weekly') ORDER BY i.id"
    ):
        if row["account_identity_id"] not in selected_ids:
            prior = source.execute("SELECT 1 FROM account_roster_members WHERE snapshot_id=? AND account_identity_id=?",
                                   (original["roster_snapshot_id"], row["account_identity_id"])).fetchone()
            pending.append({**dict(row), "reason": "not_in_source_roster" if prior is None else "source_profile_reference_not_admitted"})
    members = []
    for selected in selection:
        if selected["platform"] != "douyin":
            raise ValueError("Cleanup generation is limited to the existing Douyin routes")
        before = source.execute(
            "SELECT m.*,i.account_id,i.uid,a.enabled FROM account_roster_members m "
            "JOIN account_platform_identities i ON i.id=m.account_identity_id "
            "JOIN accounts a ON a.id=i.account_id WHERE m.snapshot_id=? AND i.id=?",
            (original["roster_snapshot_id"], selected["account_identity_id"]),
        ).fetchone()
        if before is None or not before["enabled"] or any(before[key] != selected[key] for key in IDENTITY_FIELDS):
            raise ValueError("Selected identity was not enabled in the source active roster")
        target = candidate.execute(
            "SELECT d.*,i.id account_identity_id FROM account_directory_rows d "
            "JOIN account_platform_identities i ON i.account_id=d.account_id "
            "WHERE d.account_id=? AND d.identity_status='existing_verified' AND d.account_status IN ('daily','weekly')",
            (selected["account_id"],),
        ).fetchone()
        if target is None or any(target[key] != selected[key] for key in IDENTITY_FIELDS):
            raise ValueError("Selected identity is not a matching daily/weekly verified directory member")
        references = source.execute(
            "SELECT reference_value FROM account_provider_references WHERE account_identity_id=? "
            "AND lower(provider)='tikhub' AND reference_kind='sec_user_id'", (selected["account_identity_id"],),
        ).fetchall()
        if len(references) != 1 or not _valid_douyin_sec_user_id(str(references[0][0])):
            raise ValueError("Source member lacks one valid existing TikHub sec_user_id")
        sec = str(references[0][0])
        kept = candidate.execute(
            "SELECT reference_value FROM account_provider_references WHERE account_identity_id=? "
            "AND lower(provider)='tikhub' AND reference_kind='sec_user_id'", (selected["account_identity_id"],),
        ).fetchall()
        if [str(row[0]) for row in kept] != [sec]:
            raise ValueError("Candidate did not preserve the source homepage identity reference")
        for operation in OPERATIONS:
            assignment = capture_planning.current_assignment(source, "account", str(selected["account_id"]), operation, at=prepared_at)
            if assignment is None or assignment["route"] != "integrated" or assignment["mode"] != "active" or assignment["provider"] != "tikhub":
                raise ValueError("Source member lacks an existing active integrated operation route")
        members.append({"platform": "douyin", "uid": selected["uid"], "nickname": target["nickname"],
                        "profile_ref": "https://www.douyin.com/user/" + sec, "sec_user_id": sec,
                        "monitoring_status": before["monitoring_status"], "authorization_status": before["authorization_status"],
                        "metadata": {"display_account_id": target["display_account_id"]}})
    before_gates = {table: candidate.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    for table in ("capture_paid_send_gate_events", "provider_readiness_receipts", "provider_usage")}
    accepted = seal_system_members(candidate, members, raw_root=raw_root, actor="account-cleanup",
                                   reason=f"Approved exact account-list replacement: {generation_id}", sealed_at=prepared_at)
    snapshot = candidate.execute("SELECT * FROM account_roster_snapshots WHERE id=?", (accepted["snapshot_id"],)).fetchone()
    cleanup = {"contract": CONTRACT, "generation_id": generation_id, "selection_sha256": selection_sha}
    active = append_activation(candidate, profile_id=INTEGRATED_PROFILE, roster_snapshot_id=int(snapshot["id"]),
                               roster_members_sha256=snapshot["members_sha256"], effective_at=effective_at,
                               build_receipt_sha256=build_receipt_sha256, actor="account-cleanup",
                               reason="New isolated generation for the approved account directory", metadata={"account_cleanup": cleanup},
                               created_at=prepared_at)
    # This starts an empty generation. The preserved source authority proves
    # membership separately; the native same-profile permit validates the new
    # activation against itself without pretending an old row survived reset.
    config_hashes = {json.loads(pair["gate"]["evidence_json"])["bindings"]["config_receipt_sha256"]
                     for pair in authority["operations"].values()}
    if len(config_hashes) != 1:
        raise ValueError("Source operations do not share one config receipt")
    control = {"contract": "account-cleanup-release-control-v1", "generation_id": generation_id,
               "selection_sha256": selection_sha, "active": {key: active[key] for key in ACTIVE_FIELDS},
               "transport_manifest": authority["transport_manifest"], "build_sha256": build_receipt_sha256,
               "runtime_sha256": runtime_root_receipt_sha256, "config_sha256": next(iter(config_hashes))}
    binding = {"source_activation_id": active["activation_id"], "target_activation_id": active["activation_id"],
               "business_day": parse_time(effective_at).astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat(),
               "planned_effective_at": effective_at, "build_receipt_sha256": build_receipt_sha256,
               "runtime_root_receipt_sha256": runtime_root_receipt_sha256}
    paid_drain.start_profile_drain_in_transaction(candidate, generation_id, binding=binding,
                                                switch_kind="same_profile", now=prepared_at, control=control)
    paid_drain.seal_profile_drain_in_transaction(candidate, generation_id, now=prepared_at, control=control)
    permit = paid_drain.release_profile_drain_in_transaction(candidate, generation_id, now=prepared_at, control=control)
    candidate.execute("UPDATE accounts SET enabled=0")
    route_ids = []
    for selected in selection:
        set_account_enabled_in_transaction(candidate, selected["account_identity_id"], enabled=True,
            effective_at=prepared_at, created_at=prepared_at, actor="account-cleanup", reason="Retain previously admitted directory member",
            activation_id=active["activation_id"], metadata=cleanup)
        for operation in sorted(OPERATIONS):
            route_ids.append(capture_planning.assign_route(candidate, scope_type="account", scope_key=str(selected["account_id"]),
                provider="tikhub", operation=operation, expected_generation=0, route="integrated", mode="active",
                effective_at=effective_at, recorded_at=prepared_at, account_id=selected["account_id"]))
    for table, count in before_gates.items():
        if candidate.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] != count:
            raise ValueError("Candidate preparation unexpectedly changed a provider gate or usage ledger")
    return {"contract": "account-cleanup-runtime-preparation-v1", "generation_id": generation_id,
            "active": {key: active[key] for key in ACTIVE_FIELDS}, "source_active": {key: original[key] for key in ACTIVE_FIELDS},
            "selection_sha256": selection_sha, "member_count": len(selection), "operations": sorted(OPERATIONS),
            "existing_daily_weekly_pending": pending,
            "route_count": len(route_ids), "permit_event_id": permit.event_id, "permit_event_hash": permit.event_hash,
            "release_control": control,
            "build_receipt_sha256": build_receipt_sha256, "runtime_root_receipt_sha256": runtime_root_receipt_sha256,
            "prepared_at": prepared_at, "effective_at": effective_at, "paid_gates_issued": False,
            "provider_ledger_counts": before_gates}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "candidate", "source-authority", "raw-root", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("generation-id", "build-receipt-sha256", "runtime-root-receipt-sha256", "prepared-at", "effective-at"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    source_path, candidate_path = args.source.resolve(strict=True), args.candidate.resolve(strict=True)
    if args.source.is_symlink() or args.candidate.is_symlink() or source_path == candidate_path or candidate_path.name != "candidate.sqlite3":
        parser.error("Use distinct ordinary source and offline candidate.sqlite3 files")
    if args.output.exists():
        parser.error("Preparation receipt already exists")
    authority = json.loads(args.source_authority.read_text())
    with sqlite3.connect(source_path.as_uri() + "?mode=ro", uri=True) as source, sqlite3.connect(candidate_path) as candidate:
        source.row_factory = candidate.row_factory = sqlite3.Row
        source.execute("PRAGMA query_only=ON")
        candidate.execute("PRAGMA foreign_keys=ON")
        candidate.execute("PRAGMA recursive_triggers=ON")
        candidate.execute("BEGIN IMMEDIATE")
        result = prepare_runtime(source, candidate, authority, generation_id=args.generation_id,
                                 build_receipt_sha256=args.build_receipt_sha256,
                                 runtime_root_receipt_sha256=args.runtime_root_receipt_sha256,
                                 prepared_at=args.prepared_at, effective_at=args.effective_at, raw_root=args.raw_root)
        if candidate.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise ValueError("Prepared candidate foreign keys are invalid")
        result["source_sha256"] = file_sha256(source_path)
        if result["source_sha256"] != authority["source_database_sha256"]:
            raise ValueError("Source authority database SHA differs from the retained source")
        result["source_authority_sha256"] = file_sha256(args.source_authority)
        result["candidate_identity"] = {"device": candidate_path.stat().st_dev, "inode": candidate_path.stat().st_ino}
        candidate.commit()
    result["receipt_sha256"] = auth.digest(result)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        stream.write(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ("member_count", "route_count", "active", "receipt_sha256")}, sort_keys=True))


if __name__ == "__main__":
    main()
