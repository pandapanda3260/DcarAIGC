"""Audited one-shot TikHub circuit recovery for the installed writer.

The command is preview-only by default.  A live request requires a fresh
preview fingerprint, an exact confirmation phrase and exclusive ownership of
the installed writer lock.  Isolated mode exists only for offline fixtures.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import stat
import sqlite3
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .account_roster import require_active_member, runtime_snapshot
from .capture import CaptureError, ProviderResult
from .paid_drain import dispatch_state
from .profile_activations import TIKHUB_PROFILE, activation_at
from .provider_budget import (
    PRICES_MICROUSD,
    authorize_recovery_probe,
    circuit_recovery_probe,
    circuit_state,
    finish_recovery_probe,
    paid_dispatch_owner,
    paid_scope,
)
from .providers import STAGE_CONFIG, update_content_data
from .runtime_database import (
    DatabaseAccessMode,
    InstalledWriterContract,
    ResolvedDatabaseAccess,
    RuntimeDatabaseError,
    acquire_writer_lock,
    resolve_installed_database_access,
    resolve_isolated_candidate,
)
from .source_routing import parse_time
from .storage import connect, now_utc, transaction


CONTRACT_VERSION = "tikhub-recovery-cli-v1"
PREVIEW_CONTRACT_VERSION = "tikhub-recovery-preview-v1"
OPERATION = "douyin_video_statistics"
STAGE = "metrics"
PRICE_USD = 0.001
PRICE_MICROUSD = 1_000
CONFIRMATION = "RECOVER_TIKHUB_METRICS_USD_0.001_ONCE"
PREVIEW_TTL = timedelta(minutes=5)
BEIJING = ZoneInfo("Asia/Shanghai")

CallOverride = Callable[[str, Mapping[str, Any]], ProviderResult]


class ProviderRecoveryError(RuntimeError):
    """A recovery precondition failed before an unapproved network call."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _utc(value: str) -> str:
    try:
        parsed = parse_time(value).astimezone(timezone.utc)
    except (TypeError, ValueError) as error:
        raise ProviderRecoveryError(
            "recovery_time_invalid", "A timezone-aware recovery time is required"
        ) from error
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _recovery_window(at: str) -> tuple[str, datetime, datetime]:
    local = parse_time(at).astimezone(BEIJING)
    return local.date().isoformat(), local - timedelta(days=7), local


def _published_in_window(value: Any, *, start: datetime, end: datetime) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        published = parse_time(value).astimezone(BEIJING)
    except (TypeError, ValueError):
        return False
    return start <= published <= end


def _eligible_target(
    connection: sqlite3.Connection,
    *,
    activation: Mapping[str, Any],
    content_id: int,
    business_day: str,
    published_not_before: datetime,
    published_not_after: datetime,
) -> dict[str, Any]:
    row = connection.execute(
        """SELECT c.id content_id,c.platform_content_id,c.account_id,
                  c.raw_account_uid,c.published_at,c.updated_at,
                  i.id identity_id,i.uid,a.enabled
           FROM content_items c
           JOIN account_platform_identities i
             ON i.account_id=c.account_id AND i.platform='douyin'
           JOIN accounts a ON a.id=c.account_id
           JOIN account_roster_members m
             ON m.snapshot_id=? AND m.account_identity_id=i.id
           WHERE c.platform='douyin'
             AND c.platform_content_id IS NOT NULL
             AND c.platform_content_id<>''
             AND a.enabled=1
             AND i.uid IS NOT NULL
             AND i.uid<>''
             AND (c.raw_account_uid IS NULL OR c.raw_account_uid='' OR c.raw_account_uid=i.uid)
             AND NOT EXISTS (
                 SELECT 1 FROM fetch_slots s
                 WHERE s.content_id=c.id AND s.stage=? AND s.window_key=?
             )
             AND c.id=?""",
        (int(activation["roster_snapshot_id"]), STAGE, business_day, content_id),
    ).fetchone()
    if row is None or not _published_in_window(
        row["published_at"], start=published_not_before, end=published_not_after
    ):
        raise ProviderRecoveryError(
            "recovery_target_missing",
            "The selected content is not recent-seven-day enabled Mode B Douyin "
            "work with an unused current-day metrics slot",
        )
    member = require_active_member(
        connection,
        int(row["identity_id"]),
        activation=activation,
    )
    return {
        "content_id": int(row["content_id"]),
        "platform_content_id": str(row["platform_content_id"]),
        "account_id": int(row["account_id"]),
        "identity_id": int(row["identity_id"]),
        "uid": str(member["uid"]),
        "published_at": str(row["published_at"]),
        "content_updated_at": str(row["updated_at"]),
        "metrics_window_key": business_day,
    }


def _recovery_state(
    connection: sqlite3.Connection, *, content_id: int, at: str
) -> dict[str, Any]:
    if int(connection.execute("PRAGMA user_version").fetchone()[0]) != 19:
        raise ProviderRecoveryError(
            "recovery_schema_invalid", "TikHub recovery requires schema 19"
        )
    active = activation_at(connection, at)
    if active is None or active.get("profile_id") != TIKHUB_PROFILE:
        raise ProviderRecoveryError(
            "recovery_profile_invalid",
            "The effective acquisition profile must be tikhub_managed_v1",
        )
    snapshot = runtime_snapshot(connection, active)
    paid_state = dispatch_state(connection, at=at)
    if not paid_state.paid_dispatch_open:
        raise ProviderRecoveryError(
            "recovery_dispatch_closed",
            f"Paid dispatch is {paid_state.state}: {paid_state.reason or paid_state.drain_id}",
        )
    if paid_state.activation_id not in {None, int(active["activation_id"])}:
        raise ProviderRecoveryError(
            "recovery_activation_mismatch",
            "Paid dispatch permit does not bind the effective Mode B activation",
        )
    circuit = circuit_state(connection)
    if not circuit or circuit.get("open") is not True or not circuit.get("generation"):
        raise ProviderRecoveryError(
            "recovery_circuit_not_open", "TikHub circuit is not open"
        )
    if (
        circuit.get("scope_kind") != "provider_hard"
        or circuit.get("probe_eligible") is not True
        or not circuit.get("state_fingerprint")
    ):
        raise ProviderRecoveryError(
            "recovery_fault_domain_not_probeable",
            "This recovery command only probes an eligible provider-hard fault",
        )
    if (
        STAGE_CONFIG.get(("douyin", STAGE), (None, None, None, None))[2:]
        != (OPERATION, PRICE_USD)
        or PRICES_MICROUSD.get(OPERATION) != PRICE_MICROUSD
    ):
        raise ProviderRecoveryError(
            "recovery_price_contract_changed",
            "The fixed TikHub statistics price contract has changed",
        )
    business_day, published_not_before, published_not_after = _recovery_window(at)
    target = _eligible_target(
        connection,
        activation=active,
        content_id=content_id,
        business_day=business_day,
        published_not_before=published_not_before,
        published_not_after=published_not_after,
    )
    return {
        "contract_version": CONTRACT_VERSION,
        "profile_id": TIKHUB_PROFILE,
        "activation_id": int(active["activation_id"]),
        "activation_sha256": str(active["activation_sha256"]),
        "roster_snapshot_id": int(snapshot["id"]),
        "roster_members_sha256": str(snapshot["members_sha256"]),
        "business_day": business_day,
        "operation": OPERATION,
        "price_usd": PRICE_USD,
        "max_requests": 1,
        "circuit_scope_kind": str(circuit["scope_kind"]),
        "circuit_fault_class": str(circuit["fault_class"]),
        "circuit_state_fingerprint": str(circuit["state_fingerprint"]),
        "circuit_generation": str(circuit["generation"]),
        "circuit_usage_id": circuit.get("usage_id"),
        "target": target,
    }


def _encoded_time(at: str) -> str:
    return base64.urlsafe_b64encode(at.encode("ascii")).decode("ascii").rstrip("=")


def _decoded_time(value: str) -> str:
    try:
        padded = value + "=" * (-len(value) % 4)
        return _utc(base64.urlsafe_b64decode(padded).decode("ascii"))
    except Exception as error:
        raise ProviderRecoveryError(
            "recovery_preview_invalid", "Preview fingerprint is malformed"
        ) from error


def _preview_fingerprint(state: Mapping[str, Any], *, issued_at: str) -> str:
    digest = hashlib.sha256(
        _canonical({"issued_at": issued_at, "state": dict(state)})
    ).hexdigest()
    return f"v1.{_encoded_time(issued_at)}.{digest}"


def _parse_fingerprint(value: str) -> tuple[str, str]:
    parts = str(value).split(".")
    if len(parts) != 3 or parts[0] != "v1" or len(parts[2]) != 64:
        raise ProviderRecoveryError(
            "recovery_preview_invalid", "Preview fingerprint is malformed"
        )
    if any(character not in "0123456789abcdef" for character in parts[2]):
        raise ProviderRecoveryError(
            "recovery_preview_invalid", "Preview fingerprint is malformed"
        )
    return _decoded_time(parts[1]), parts[2]


def preview_recovery(
    database: Path, *, content_id: int, at: str
) -> dict[str, Any]:
    issued_at = _utc(at)
    with connect(database) as connection:
        state = _recovery_state(connection, content_id=content_id, at=issued_at)
    fingerprint = _preview_fingerprint(state, issued_at=issued_at)
    return {
        "status": "preview",
        "preview_contract_version": PREVIEW_CONTRACT_VERSION,
        "issued_at": issued_at,
        "expires_at": _utc(
            (parse_time(issued_at) + PREVIEW_TTL).isoformat()
        ),
        "preview_fingerprint": fingerprint,
        "required_confirmation": CONFIRMATION,
        **state,
    }


def _validate_fingerprint(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    value: str,
    at: str,
) -> dict[str, Any]:
    issued_at, supplied_digest = _parse_fingerprint(value)
    issued = parse_time(issued_at)
    current = parse_time(at)
    if current < issued or current - issued > PREVIEW_TTL:
        raise ProviderRecoveryError(
            "recovery_preview_expired",
            "Preview fingerprint is expired or from the future",
        )
    state = _recovery_state(connection, content_id=content_id, at=at)
    expected = _preview_fingerprint(state, issued_at=issued_at).rsplit(".", 1)[1]
    if not hmac.compare_digest(supplied_digest, expected):
        raise ProviderRecoveryError(
            "recovery_preview_stale",
            "Recovery state changed after preview; generate a new preview",
        )
    return state


def apply_recovery(
    database: Path,
    *,
    content_id: int,
    preview_fingerprint: str,
    confirmation: str,
    at: str,
    call_override: CallOverride | None = None,
) -> dict[str, Any]:
    if confirmation != CONFIRMATION:
        raise ProviderRecoveryError(
            "recovery_confirmation_required",
            f"Live recovery requires --confirm {CONFIRMATION}",
        )
    timestamp = _utc(at)
    with connect(database) as connection, transaction(connection):
        state = _validate_fingerprint(
            connection,
            content_id=content_id,
            value=preview_fingerprint,
            at=timestamp,
        )
        probe = authorize_recovery_probe(
            connection,
            authorization_ref=f"{CONTRACT_VERSION}:{preview_fingerprint.rsplit('.', 1)[1]}",
            operation=OPERATION,
            at=timestamp,
            scope_kind=str(state["circuit_scope_kind"]),
        )
    target = dict(state["target"])
    task_id = (
        f"tikhub-recovery:{state['circuit_generation']}:"
        f"{state['circuit_state_fingerprint'][:16]}:"
        f"{state['business_day']}:{target['content_id']}"
    )
    try:
        with paid_scope(
            "metrics",
            activation_id=int(state["activation_id"]),
            roster_snapshot_id=int(state["roster_snapshot_id"]),
            roster_snapshot_hash=str(state["roster_members_sha256"]),
            business_day=str(state["business_day"]),
        ), paid_dispatch_owner(
            job_id="provider_recovery_probe:tikhub",
            identity={
                "contract_version": CONTRACT_VERSION,
                "purpose": "metrics",
                "business_day": str(state["business_day"]),
                "provider": "tikhub",
                "operation": OPERATION,
                "fault_scope_kind": str(state["circuit_scope_kind"]),
                "fault_state_fingerprint": str(
                    state["circuit_state_fingerprint"]
                ),
                "fault_generation": str(state["circuit_generation"]),
                "content_id": int(target["content_id"]),
            },
            db_path=database,
            at=timestamp,
        ), circuit_recovery_probe(
            int(probe["id"]), defer_completion=True
        ):
            update = update_content_data(
                int(target["content_id"]),
                as_of=parse_time(timestamp).astimezone(BEIJING).date(),
                db_path=database,
                call_override=call_override,
                stages=[STAGE],
                process_media=False,
                task_id=task_id,
                task_max_amount=PRICE_USD,
            )
    except CaptureError as error:
        update = {
            "status": "partial",
            "stages": [
                {
                    "stage": STAGE,
                    "status": "failed",
                    "error_code": error.error_code,
                    "message": str(error),
                }
            ],
            "provider_cost": PRICE_USD if error.billed is not False else 0.0,
        }
    with connect(database) as connection, transaction(connection):
        row = connection.execute(
            "SELECT status,details_json FROM scheduler_runs WHERE id=?",
            (int(probe["id"]),),
        ).fetchone()
        proof = json.loads(str(row["details_json"])) if row is not None else {}
        usage = None
        if type(proof.get("usage_id")) is int:
            usage_row = connection.execute(
                "SELECT * FROM provider_usage WHERE id=?", (proof["usage_id"],)
            ).fetchone()
            if usage_row is not None:
                usage = dict(usage_row)
                usage["details"] = json.loads(str(usage.pop("details_json")))
        raw_response_id = (
            usage["details"].get("raw_response_id") if usage is not None else None
        )
        raw = None
        observation = None
        if type(raw_response_id) is int:
            raw_row = connection.execute(
                "SELECT * FROM provider_raw_responses WHERE id=?",
                (raw_response_id,),
            ).fetchone()
            raw = dict(raw_row) if raw_row is not None else None
            observation_row = connection.execute(
                """SELECT * FROM content_metric_observations
                   WHERE content_id=? AND window_key=? AND raw_response_id=?
                     AND observation_origin='provider_capture'
                   ORDER BY id DESC LIMIT 1""",
                (
                    int(target["content_id"]),
                    str(state["business_day"]),
                    raw_response_id,
                ),
            ).fetchone()
            observation = (
                dict(observation_row) if observation_row is not None else None
            )
        verified_raw_response_id = (
            raw_response_id if type(raw_response_id) is int else None
        )
        materialization_verified = (
            update.get("status") == "succeeded"
            and proof.get("state") == "sent"
            and usage is not None
            and usage["operation"] == OPERATION
            and int(usage["request_attempts"]) == 1
            and usage["details"].get("state") == "completed"
            and verified_raw_response_id is not None
            and raw is not None
            and str(raw["provider"]).lower() == "tikhub"
            and raw["operation"] == OPERATION
            and raw["source"] == "live_applied"
            and 200 <= int(raw["http_status"] or 0) < 300
            and observation is not None
            and observation["source"] == "tikhub"
        )
        if usage is not None and proof.get("state") in {"reserved", "sent"}:
            failure_reason = next(
                (
                    str(stage.get("error_code"))
                    for stage in update.get("stages", [])
                    if stage.get("status") == "failed" and stage.get("error_code")
                ),
                "recovery_materialization_unverified",
            )
            finish_recovery_probe(
                connection,
                usage_id=int(usage["id"]),
                succeeded=materialization_verified,
                at=now_utc(),
                raw_response_id=(
                    verified_raw_response_id if materialization_verified else None
                ),
                reason=None if materialization_verified else failure_reason,
            )
        final_row = connection.execute(
            "SELECT details_json FROM scheduler_runs WHERE id=?", (int(probe["id"]),)
        ).fetchone()
        proof = json.loads(str(final_row["details_json"])) if final_row else {}
        current_circuit = circuit_state(connection) or {}
    succeeded = (
        materialization_verified
        and proof.get("state") == "succeeded"
        and current_circuit.get("open") is False
        and usage is not None
        and usage["operation"] == OPERATION
        and int(usage["request_attempts"]) == 1
        and usage["details"].get("state") == "completed"
    )
    return {
        "status": "succeeded" if succeeded else "failed",
        "contract_version": CONTRACT_VERSION,
        "probe_id": int(probe["id"]),
        "operation": OPERATION,
        "authorized_price_usd": PRICE_USD,
        "target": target,
        "update": update,
        "probe": proof,
        "circuit_open": current_circuit.get("open"),
        "usage": usage,
        "materialization_verified": materialization_verified,
        "automatic_retry": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--content-id", type=int, required=True)
    parser.add_argument("--project-root", type=Path)
    parser.add_argument("--isolated", action="store_true")
    parser.add_argument("--at", help="isolated fixtures only")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--preview-fingerprint")
    return parser


def _access(arguments: argparse.Namespace) -> ResolvedDatabaseAccess:
    if arguments.isolated:
        return resolve_isolated_candidate(arguments.db)
    if arguments.project_root is None:
        raise ProviderRecoveryError(
            "formal_database_identity_unresolved",
            "Formal recovery requires --project-root",
        )
    try:
        return resolve_installed_database_access(
            DatabaseAccessMode.FORMAL_MUTATION,
            database=arguments.db,
            project_root=arguments.project_root,
        )
    except RuntimeDatabaseError as error:
        raise ProviderRecoveryError(
            "formal_database_identity_unresolved", str(error)
        ) from error


def _private_regular_file(value: object, *, label: str) -> Path:
    if not isinstance(value, (str, os.PathLike)):
        raise ProviderRecoveryError(
            "recovery_credential_identity_unresolved", f"{label} is missing"
        )
    path = Path(value).expanduser()
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise ProviderRecoveryError(
            "recovery_credential_identity_unresolved",
            f"{label} must be an absolute regular non-symlink file",
        )
    if stat.S_IMODE(path.stat().st_mode) not in {0o400, 0o600}:
        raise ProviderRecoveryError(
            "recovery_credential_identity_unresolved",
            f"{label} must have mode 0400 or 0600",
        )
    return path.resolve(strict=True)


def _validate_formal_key_binding(
    installed: InstalledWriterContract,
    *,
    environ: Mapping[str, str],
) -> Path:
    payload_environment = installed.payload.get("EnvironmentVariables")
    if not isinstance(payload_environment, Mapping):
        raise ProviderRecoveryError(
            "recovery_credential_identity_unresolved",
            "Installed writer environment is invalid",
        )
    writer_env = _private_regular_file(
        payload_environment.get("DCAR_WRITER_ENV_FILE"),
        label="installed DCAR_WRITER_ENV_FILE",
    )
    configured: list[str] = []
    for raw_line in writer_env.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.removesuffix("\r")
        if not line or line.startswith("#"):
            continue
        if line.startswith("TIKHUB_API_KEY_FILE="):
            configured.append(line.removeprefix("TIKHUB_API_KEY_FILE="))
    if len(configured) != 1:
        raise ProviderRecoveryError(
            "recovery_credential_identity_unresolved",
            "Installed writer environment must contain one TIKHUB_API_KEY_FILE",
        )
    installed_key = _private_regular_file(
        configured[0], label="installed TIKHUB_API_KEY_FILE"
    )
    process_value = environ.get("TIKHUB_API_KEY_FILE", "").strip()
    process_key = _private_regular_file(
        process_value, label="process TIKHUB_API_KEY_FILE"
    )
    if process_key != installed_key:
        raise ProviderRecoveryError(
            "recovery_credential_identity_unresolved",
            "Process TIKHUB_API_KEY_FILE does not match the installed writer",
        )
    if environ.get("TIKHUB_API_KEY", "").strip():
        raise ProviderRecoveryError(
            "recovery_credential_identity_unresolved",
            "Formal recovery forbids a direct TIKHUB_API_KEY override",
        )
    return installed_key


def main(
    argv: Sequence[str] | None = None,
    *,
    call_override: CallOverride | None = None,
) -> int:
    arguments = _parser().parse_args(argv)
    try:
        access = _access(arguments)
        if not arguments.isolated and arguments.at is not None:
            raise ProviderRecoveryError(
                "recovery_clock_override",
                "Formal recovery cannot override the clock",
            )
        if call_override is not None and not arguments.isolated:
            raise ProviderRecoveryError(
                "recovery_override_forbidden",
                "Provider call overrides are isolated-test only",
            )
        if arguments.isolated and arguments.apply and call_override is None:
            raise ProviderRecoveryError(
                "recovery_isolated_live_call_forbidden",
                "Isolated apply requires an in-process provider call override",
            )
        if arguments.apply:
            if not arguments.preview_fingerprint:
                raise ProviderRecoveryError(
                    "recovery_preview_required",
                    "Live recovery requires --preview-fingerprint",
                )
            if not arguments.confirm:
                raise ProviderRecoveryError(
                    "recovery_confirmation_required",
                    f"Live recovery requires --confirm {CONFIRMATION}",
                )
        elif arguments.confirm or arguments.preview_fingerprint:
            raise ProviderRecoveryError(
                "recovery_apply_required",
                "Confirmation and fingerprint are valid only with --apply",
            )
        timestamp = _utc(arguments.at or now_utc())
        lock = (
            nullcontext()
            if access.access_mode is DatabaseAccessMode.ISOLATED_CANDIDATE
            else acquire_writer_lock(access)
        )
        with lock:
            if arguments.apply:
                if not arguments.isolated:
                    if access.installed is None:
                        raise ProviderRecoveryError(
                            "recovery_credential_identity_unresolved",
                            "Installed writer contract is required for formal recovery",
                        )
                    _validate_formal_key_binding(
                        access.installed, environ=os.environ
                    )
                result = apply_recovery(
                    access.database,
                    content_id=arguments.content_id,
                    preview_fingerprint=str(arguments.preview_fingerprint),
                    confirmation=str(arguments.confirm),
                    at=timestamp,
                    call_override=call_override,
                )
            else:
                result = preview_recovery(
                    access.database, content_id=arguments.content_id, at=timestamp
                )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
        return 0 if result["status"] in {"preview", "succeeded"} else 2
    except (ProviderRecoveryError, RuntimeDatabaseError) as error:
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "error_code": getattr(error, "code", type(error).__name__),
                    "message": str(error),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
