"""Explicitly synthetic billing evidence for isolated historical canary tests."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from contextlib import closing
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch
from zoneinfo import ZoneInfo

from v8 import billing_reconciliation
from v8.capture import BILLING_UNKNOWN_SLOT_ERROR
from v8.paid_identity import build_paid_request_identity
from v8.provider_budget import authorize_compensation, budget_day, record_compensation_gap
from v8.runtime_database import resolve_isolated_candidate
from v8.storage import connect, require_schema_compatibility, transaction


def seed_offline_compensation(
    db: Path,
    target_id: int = 1,
    account_id: int = 39,
    subject: str = "canary-1",
    *,
    at: str | None = None,
) -> dict[str, Any]:
    """Seed, genuinely settle, and authorize one synthetic sequence-0 send.

    Only a temporary schema-18 fixture is eligible. The returned authorization
    remains unconsumed: the actual canary claim must consume sequence 1 itself.
    No transport, provider bill, or production evidence is obtained or asserted.
    """
    if os.environ.get("DCAR_TEST_DENY_FORMAL_DB") != "1":
        raise ValueError("offline compensation requires DCAR_TEST_DENY_FORMAL_DB=1")
    candidate = Path(db).absolute()
    if candidate.is_symlink():
        raise ValueError("offline compensation fixture must not be a symlink")
    candidate = candidate.resolve(strict=True)
    temporary_roots = {Path(tempfile.gettempdir()).resolve(), Path("/tmp").resolve()}
    if not any(candidate.is_relative_to(root) for root in temporary_roots):
        raise ValueError("offline compensation requires a temporary database")
    candidate = resolve_isolated_candidate(candidate).database
    instant = datetime.fromisoformat(at.replace("Z", "+00:00")) if at else datetime.now(timezone.utc)
    if instant.tzinfo is None:
        raise ValueError("offline compensation time must include a timezone")
    instant = instant.astimezone(timezone.utc).replace(microsecond=0)
    recorded_at = instant.isoformat(timespec="seconds").replace("+00:00", "Z")
    beijing = ZoneInfo("Asia/Shanghai")
    next_midnight = datetime.combine(
        instant.astimezone(beijing).date() + timedelta(days=1), time.min, tzinfo=beijing
    )
    expires_at = (next_midnight - timedelta(microseconds=1)).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    original_identity = build_paid_request_identity(
        provider="TikHub",
        operation="douyin_video_detail",
        platform="douyin",
        subject=subject,
        request_parameters={"aweme_id": subject},
        cursor=None,
        due_bucket=f"offline-history:{subject}",
    )
    batch_id = f"offline-fixture-original-{target_id}-{original_identity.scope_identity[:16]}"
    with closing(connect(candidate)) as connection, transaction(connection):
        require_schema_compatibility(connection, supported_versions=frozenset({18}))
        target = connection.execute(
            "SELECT account_id,platform,platform_content_id FROM content_items WHERE id=?",
            (target_id,),
        ).fetchone()
        if target is None or tuple(target) != (account_id, "douyin", subject):
            raise ValueError("offline compensation target does not match the fixture")
        connection.execute(
            """INSERT INTO provider_budget_batches(
                   id,purpose,provider,operation,currency,verified_unit_price,
                   max_billable_requests,max_amount,pilot_size,daily_quota,
                   price_verified_at,status,consumed_requests,consumed_amount,
                   created_at,updated_at)
               VALUES (?,?,'TikHub','douyin_video_detail','USD',.001,
                       1,.001,1,1,?,'approved',1,.001,?,?)""",
            (batch_id, batch_id, recorded_at, recorded_at, recorded_at),
        )
        slot = connection.execute(
            """INSERT INTO fetch_slots(
                   content_id,stage,window_key,provider,adapter_version,status,
                   attempt_count,last_error_code,last_error_message,
                   started_at,finished_at,created_at,updated_at)
               VALUES (?,'detail',?,'TikHub','offline-fixture-v1',
                       'retryable_failed',1,?,'synthetic offline unknown billing',?,?,?,?)""",
            (
                target_id, original_identity.document["due_bucket"],
                BILLING_UNKNOWN_SLOT_ERROR, recorded_at, recorded_at, recorded_at, recorded_at,
            ),
        )
        assert slot.lastrowid is not None
        original_slot_id = int(slot.lastrowid)
        attempt = connection.execute(
            """INSERT INTO fetch_attempts(
                   slot_id,attempt_number,request_started_at,response_finished_at,
                   billed,amount,currency,error_code,error_message)
               VALUES (?,1,?,?,0,NULL,'USD','transport_error','synthetic offline timeout')""",
            (original_slot_id, recorded_at, recorded_at),
        )
        assert attempt.lastrowid is not None
        original_attempt_id = int(attempt.lastrowid)
        usage = connection.execute(
            """INSERT INTO provider_usage(
                   task_id,budget_batch_id,provider,operation,request_attempts,
                   billed_requests,currency,amount,recorded_at,details_json)
               VALUES (?,?,'TikHub','douyin_video_detail',1,1,'USD',.001,?,?)""",
            (
                batch_id, batch_id, recorded_at,
                json.dumps({
                    "state": "billing_unknown",
                    "slot_id": original_slot_id,
                    "attempt_number": 1,
                    "category": "detail",
                    "budget_day": budget_day(recorded_at),
                    "error_code": "transport_error",
                    "paid_identity": original_identity.document,
                    "paid_scope_identity": original_identity.scope_identity,
                    "paid_execution_identity": original_identity.execution_identity,
                    "paid_sequence": 0,
                    "offline_fixture": True,
                }, sort_keys=True),
            ),
        )
        assert usage.lastrowid is not None
        original_usage_id = int(usage.lastrowid)

    evidence_ref = f"offline-fixture-billing:{original_identity.scope_identity}:{original_usage_id}"
    operator_ref = "offline-fixture-operator"
    with closing(sqlite3.connect(candidate.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        preview = billing_reconciliation.preview_unknown_billing(
            connection,
            usage_id=original_usage_id,
            expected_slot_id=original_slot_id,
            expected_attempt_number=1,
            outcome="billed",
            evidence_ref=evidence_ref,
            operator_ref=operator_ref,
        )
    with patch.object(billing_reconciliation, "now_utc", return_value=recorded_at):
        settlement = billing_reconciliation.reconcile_unknown_billing(
            db_path=candidate,
            usage_id=original_usage_id,
            expected_slot_id=original_slot_id,
            expected_attempt_number=1,
            outcome="billed",
            evidence_ref=evidence_ref,
            operator_ref=operator_ref,
            expected_fingerprint=str(preview["fingerprint"]),
            isolated=True,
        )
    with closing(connect(candidate)) as connection, transaction(connection):
        gap = record_compensation_gap(
            connection,
            original_usage_id=original_usage_id,
            paid_scope_identity=original_identity.scope_identity,
            operation="douyin_video_detail",
            local_replay_exhausted=True,
            raw_unrecoverable=True,
            business_gap_due=True,
            at=recorded_at,
        )
        authorization = authorize_compensation(
            connection,
            original_usage_id=original_usage_id,
            paid_scope_identity=original_identity.scope_identity,
            operation="douyin_video_detail",
            reason="offline fixture: synthetic timeout has no raw to replay",
            owner=operator_ref,
            gap_evidence_id=int(gap["id"]),
            expires_at=expires_at,
            at=recorded_at,
        )
    return {
        "authorization": authorization,
        "original_identity": original_identity,
        "original_usage_id": original_usage_id,
        "original_slot_id": original_slot_id,
        "original_attempt_id": original_attempt_id,
        "settlement": settlement,
        "at": recorded_at,
    }
