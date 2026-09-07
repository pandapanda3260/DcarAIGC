"""Freeze historical large-page accounts before any diagnostic outcomes exist.

Historical completeness means a hash-verified stored JSON page accepted by the
existing fixed-route parser, not a claim that the historical HTTP transport was
qualified. This module never creates paid work or grants send authority.
"""

from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path
from typing import Any

from .capture import CaptureError, StoredRawResponse
from .raw_evidence import RawEvidenceError, read_raw_evidence
from .source_routing import parse_time
from .storage import PROJECT_ROOT
from .tikhub_scan import TikHubScanError, _page
from .transport_hold_binding import read_current_diagnostic_hold
from .transport_receipts import append_transport_receipt, read_transport_receipt

CONTRACT_VERSION = "large_page_account_cohort_v1"
_COLUMNS = [
    "identity_id",
    "account_id",
    "uid",
    "raw_id",
    "entity_bytes",
    "entity_sha256",
]


class TransportCohortError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _latest_complete(
    connection: sqlite3.Connection,
    *,
    account_id: int,
    raw_hwm: int,
    at: str,
) -> tuple[list[Any] | None, int]:
    rows = connection.execute(
        "SELECT * FROM provider_raw_responses WHERE account_id=? "
        "AND provider='TikHub' AND operation='douyin_user_posts' AND id<=? "
        "AND http_status=200 AND julianday(captured_at)<=julianday(?) "
        "ORDER BY julianday(captured_at) DESC,id DESC",
        (account_id, raw_hwm, at),
    )
    rejected_shapes = 0
    for row in rows:
        # Integrity failures cannot silently shrink an account's P75 input.
        local_path = Path(str(row["local_path"]))
        resolved = local_path if local_path.is_absolute() else PROJECT_ROOT / local_path
        try:
            evidence = read_raw_evidence(
                resolved,
                expected_stored_sha256=row["sha256"],
                expected_stored_size=row["byte_size"],
            )
        except (OSError, ValueError, RawEvidenceError) as exc:
            raise TransportCohortError(
                "transport_cohort_raw_invalid",
                f"Historical raw {row['id']} failed integrity readback",
            ) from exc
        try:
            value = json.loads(evidence.entity_bytes)
            raw = StoredRawResponse(
                slot_id=0,
                raw_response_id=row["id"],
                provider="TikHub",
                operation="douyin_user_posts",
                value=value,
                http_status=200,
                captured_at=row["captured_at"],
                sha256=row["sha256"],
                local_path=resolved,
            )
            _items, more, cursor, _total = _page(raw, "douyin")
            if more and cursor in (None, ""):
                raise ValueError("Nonterminal historical page has no cursor")
        except (ValueError, UnicodeError, CaptureError, TikHubScanError):
            rejected_shapes += 1
            continue
        return [
            int(row["id"]),
            evidence.receipt.entity_size,
            evidence.receipt.entity_sha256,
        ], rejected_shapes
    return None, rejected_shapes


def freeze_large_page_cohort(
    connection: sqlite3.Connection,
    *,
    drain_id: str,
    at: str,
    mirror_root: Path,
) -> dict[str, Any]:
    """Freeze once per HOLD/build generation, under the caller write transaction.

    The nearest-rank P75 denominator is one latest complete page per roster
    Douyin account, not all historical pages. Ties at P75 are included. Accounts
    without eligible raw are explicitly excluded and cannot fill a short sample.
    A retry returns the original receipt and never reselects using later raw.
    """

    if not connection.in_transaction:
        raise TransportCohortError(
            "transport_cohort_transaction_required",
            "Cohort freeze requires a transaction",
        )
    hold = read_current_diagnostic_hold(connection, drain_id=drain_id, at=at)
    identity_key = f"{CONTRACT_VERSION}:{hold['start_event_hash']}:{hold['generation']}"
    existing = connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:cohort' "
        "AND scheduled_for=?",
        (identity_key,),
    ).fetchone()
    if existing:
        receipt = read_transport_receipt(connection, int(existing["id"]))
        if receipt["payload"].get("hold_binding") != hold:
            raise TransportCohortError(
                "transport_cohort_hold_changed",
                "Frozen cohort belongs to another HOLD binding",
            )
        return receipt
    members = connection.execute(
        "SELECT m.account_identity_id,m.uid,i.account_id,i.uid AS current_uid "
        "FROM account_roster_members m JOIN account_platform_identities i "
        "ON i.id=m.account_identity_id WHERE m.snapshot_id=? AND m.platform='douyin' "
        "ORDER BY m.uid,m.account_identity_id",
        (hold["roster_snapshot_id"],),
    ).fetchall()
    raw_hwm = int(
        connection.execute(
            "SELECT COALESCE(MAX(id),0) FROM provider_raw_responses"
        ).fetchone()[0]
    )
    records: list[list[Any]] = []
    missing: list[int] = []
    invalid_shape_count = 0
    for member in members:
        if not member["uid"] or member["uid"] != member["current_uid"]:
            raise TransportCohortError(
                "transport_cohort_identity_changed",
                "Roster account identity is no longer exact",
            )
        latest, rejected = _latest_complete(
            connection,
            account_id=int(member["account_id"]),
            raw_hwm=raw_hwm,
            at=at,
        )
        invalid_shape_count += rejected
        if latest is None:
            missing.append(int(member["account_identity_id"]))
        else:
            records.append(
                [
                    int(member["account_identity_id"]),
                    int(member["account_id"]),
                    str(member["uid"]),
                    *latest,
                ]
            )
    if not records:
        raise TransportCohortError(
            "transport_cohort_empty",
            "No complete historical Douyin page is available",
        )
    sizes = sorted(int(record[4]) for record in records)
    nearest_rank = math.ceil(len(sizes) * 0.75)
    threshold = sizes[nearest_rank - 1]
    payload = {
        "contract_version": CONTRACT_VERSION,
        "hold_binding": hold,
        "frozen_at": parse_time(at).isoformat(),
        "raw_high_watermark": raw_hwm,
        "percentile_rule": "nearest_rank_ceil_n_times_0.75_include_ties",
        "p75_entity_bytes": threshold,
        "p75_rank": nearest_rank,
        "complete_page_account_count": len(records),
        "roster_douyin_account_count": len(members),
        # Columnar compact records keep a 325-account manifest below the
        # existing immutable-receipt 64 KiB bound without hiding evidence.
        "record_columns": _COLUMNS,
        "records": records,
        "selected_identity_ids": [
            int(row[0]) for row in records if row[4] >= threshold
        ],
        "missing_complete_raw_identity_ids": missing,
        "rejected_raw_shape_count": invalid_shape_count,
        "historical_completeness": "stored_hash_and_fixed_route_page_shape_only",
    }
    return append_transport_receipt(
        connection,
        kind="cohort",
        identity_key=identity_key,
        payload=payload,
        at=at,
        mirror_root=mirror_root,
    )
