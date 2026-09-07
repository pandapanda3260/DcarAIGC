"""Synthetic qualification only for isolated boundary tests, never deployment."""
from pathlib import Path

from v8 import capture_authorizations as auth


def local_storage_receipt(root: Path, *, daily_stored_p95: int = 1024) -> dict:
    """Use the real temporary disk; this is never a fabricated archive mount."""
    from v8.local_raw_retention import qualify_local_storage

    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return qualify_local_storage(live_root=root.resolve(), daily_stored_p95=daily_stored_p95)


def qualify(connection, bindings, operation, at, expires):
    bucket = "discovery" if operation in auth.provider_budget.DISCOVERY_OPERATIONS else "metrics"
    scope = auth.scope_hash(runtime_bindings=bindings, provider="tikhub", operation=operation)
    evidence = {"contract": auth.READINESS_CONTRACT, "bindings": bindings, "scope_hash": scope,
        "qualification": "qualified", "continuity_permit_sha256": bindings["continuity_permit_sha256"],
        "transport_manifest_sha256": "f"*64}
    ready = {"provider": "tikhub", "operation": operation, "status": "ready", "reason": "isolated-fixture",
        "evidence_json": auth.canonical(evidence), "created_at": at, "expires_at": expires}
    ready_id = connection.execute("INSERT INTO provider_readiness_receipts(provider,operation,status,reason,evidence_json,created_at,expires_at,receipt_sha256) VALUES(:provider,:operation,:status,:reason,:evidence_json,:created_at,:expires_at,:sha)",
        {**ready, "sha": auth.digest(ready)}).lastrowid
    payload = {"contract": auth.CONTRACT, "bindings": bindings, "scope_hash": scope, "operation": operation,
        "issued_at": at, "expires_at": expires, "readiness_receipt_id": ready_id,
        "readiness_receipt_sha256": auth.digest(ready), "transport_manifest_sha256": "f"*64,
        "budget": {"total_microusd": 50_000_000,
                   "bucket_microusd": auth.provider_budget.BUDGET_BUCKET_MICROUSD[bucket], "bucket": bucket}}
    gate = {"provider": "tikhub", "operation": operation, "state": "open", "reason": "isolated-fixture",
        "evidence_json": auth.canonical(payload), "recorded_at": at}
    connection.execute("INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(:provider,:operation,:state,:reason,:evidence_json,:recorded_at,:sha)",
        {**gate, "sha": auth.digest(gate)})
