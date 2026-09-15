"""Plan once per authority generation; verify files outside the writer lock."""
from __future__ import annotations

from datetime import timedelta
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from . import account_catalog_capture as catalog, catalog_revision, capture_planning as planning
from .profile_activations import activation_at
from .storage import connect, transaction


class PlanDeferred(ValueError):
    pass


def inputs(connection: sqlite3.Connection, active: Mapping[str, Any], policy: Mapping[str, Any] | None,
           *, at: str, shadow: bool) -> dict[str, Any]:
    from .capture_runtime import _time, BEIJING
    local = _time(at).astimezone(BEIJING)
    day = local.date() - timedelta(days=int((local.hour, local.minute) < (0, 10)))
    return {"contract":"capture-plan-reuse-v1", "business_day":day.isoformat(),
        **{k:active[k] for k in ("activation_id","profile_id","activation_sha256","roster_snapshot_id","roster_members_sha256")},
        "catalog_revision":catalog_revision.revision(connection),
        "catalog_policy_sha256":planning.digest(policy) if policy is not None else None,
        "mode":"shadow" if shadow else "active"}


def stored(connection: sqlite3.Connection, key: dict[str, Any]) -> dict[str, Any] | None:
    row = connection.execute("SELECT p.id,p.payload_json,p.plan_sha256,r.input_json FROM capture_plan_reuse r "
        "JOIN capture_source_plans p ON p.id=r.source_plan_id WHERE r.reuse_key=?", (planning.digest(key),)).fetchone()
    if row is None:
        return None
    payload = json.loads(row["payload_json"])
    if json.loads(row["input_json"]) != key or planning.digest(payload) != row["plan_sha256"]:
        raise ValueError("Stored capture plan reuse proof changed")
    return {"id":row["id"], **payload}


def current(connection: sqlite3.Connection, prepared: Mapping[str, Any], *, at: str) -> bool:
    active = activation_at(connection, at)
    return active is not None and inputs(connection, active, prepared["policy"], at=at,
        shadow=prepared["shadow"]) == prepared["key"]


def ensure(db_path: Path, *, at: str, shadow: bool = False) -> dict[str, Any]:
    """Return a verified snapshot plus its key, never a permission to send."""
    from .capture_runtime import _cohort_plan
    from .runtime_evidence_context import _PREPARED, inheritance_boundary, prepare_inheritance
    from .storage import live_wal_read_only_connections

    for _attempt in range(2):
        # Standalone retries prepare afresh before any snapshot or writer lock.
        # Materializing a paid page is still inside that work's A/reserve/B
        # context: borrow its unchanged logical scope through the usual owner,
        # database and non-nested-boundary checks, then fence it again below.
        inherited = _PREPARED.get()
        logical_at = inherited.logical_at if inherited is not None else at
        with prepare_inheritance(db_path, logical_at=logical_at):
            with live_wal_read_only_connections(), connect(db_path, read_only=True) as connection:
                connection.execute("BEGIN")
                try:
                    with inheritance_boundary(connection):
                        active = activation_at(connection, at)
                        if active is None:
                            raise PlanDeferred("no_activation")
                        selected_shadow = shadow or active["profile_id"] != "integrated_route_v1"
                        policy = catalog.installed_policy(connection, at=at)
                        key = inputs(connection, active, policy, at=at, shadow=selected_shadow)
                        old = stored(connection, key)
                        if old is not None:
                            return {"plan":old,"key":key,"policy":policy,"shadow":selected_shadow,"reused":True}
                        snapshot = catalog.freeze_snapshot(connection, policy=policy) if policy is not None else None
                        locators = catalog.prepare_proven_locators(connection, snapshot["eligibility"]["eligible_members"]) if snapshot else {}
                finally:
                    # Entry and exit fences must run while the same read
                    # snapshot remains open, including early-return paths.
                    connection.rollback()
            prepared = {"key":key,"policy":policy,"snapshot":snapshot,"locators":locators,"shadow":selected_shadow}
            with connect(db_path) as connection, transaction(connection), inheritance_boundary(connection):
                if not current(connection, prepared, at=at):
                    continue
                old = stored(connection, key)
                if old is not None:
                    return {**prepared,"plan":old,"reused":True}
                with catalog_revision.projection(connection):
                    plan = _cohort_plan(connection, active, at=at, shadow=selected_shadow, prepared=prepared)
                connection.execute("INSERT INTO capture_plan_reuse(reuse_key,source_plan_id,input_json,created_at) VALUES(?,?,?,?)",
                    (planning.digest(key),plan["id"],planning.canonical(key),planning.timestamp(at)))
                return {**prepared,"plan":plan,"reused":False}
    raise PlanDeferred("catalog_changed_during_planning")
