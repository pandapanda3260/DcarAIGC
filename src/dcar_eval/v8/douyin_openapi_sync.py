"""Authorization-only Douyin OpenAPI health for the Mac writer."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from .douyin_openapi_client import DouyinMachineClient
from .storage import DEFAULT_DB, connect, now_utc


AUTHORIZATION_CONTRACT_VERSION = "authorization-status-v1"


def reconcile_with_client(
    *,
    scheduled_for: datetime,
    db_path: Path,
    client: DouyinMachineClient,
    raw_root: Path | None = None,
    materialize_page: Callable[..., Mapping[str, Any]] | None = None,
) -> Mapping[str, Any]:
    """Read authorization state; legacy materializer arguments have no effect."""
    if scheduled_for.tzinfo is None:
        raise ValueError("scheduled_for must be timezone-aware")
    del raw_root, materialize_page
    authorizations = client.list_authorizations()
    captured_at = now_utc()
    captured_epoch = int(datetime.fromisoformat(captured_at.replace("Z", "+00:00")).timestamp())
    accounts = []
    with connect(db_path) as connection:
        for authorization in authorizations:
            account_id = int(authorization["account_id"])
            uid = str(authorization["platform_uid"])
            matched = connection.execute(
                "SELECT 1 FROM account_platform_identities i "
                "WHERE i.account_id=? AND i.platform='douyin' AND i.uid=?",
                (account_id, uid),
            ).fetchone() is not None
            needs_reauthorization = authorization["needs_reauthorization"]
            access_expires_at = authorization["access_expires_at"]
            refresh_expires_at = authorization["refresh_expires_at"]
            if not matched:
                reason = "authorization_identity_mismatch"
            elif needs_reauthorization:
                reason = "reauthorization_required"
            elif refresh_expires_at <= captured_epoch:
                reason = "refresh_expired"
            elif access_expires_at <= captured_epoch:
                reason = "access_expired"
            else:
                reason = ""
            accounts.append({
                "authorization_id": str(authorization["authorization_id"]),
                "account_id": account_id,
                "platform_uid": uid,
                "status": "attention" if reason else "available",
                "identity_matches": matched,
                "needs_reauthorization": needs_reauthorization,
                "access_expires_at": access_expires_at,
                "refresh_expires_at": refresh_expires_at,
                "reason": reason,
            })
    state = (
        "no_authorization" if not accounts
        else "available" if all(row["status"] == "available" for row in accounts)
        else "attention"
    )
    return {
        "contract_version": AUTHORIZATION_CONTRACT_VERSION,
        "captured_at": captured_at,
        "content_sync_enabled": False,
        "authorization_state": state,
        "accounts": accounts,
    }


def run_douyin_openapi_reconcile(
    *, scheduled_for: datetime, db_path: Path = DEFAULT_DB
) -> Mapping[str, Any]:
    """Keep the public entry point, restricted to the authorization directory."""
    with DouyinMachineClient.from_env() as client:
        return reconcile_with_client(
            scheduled_for=scheduled_for,
            db_path=db_path,
            client=client,
        )
