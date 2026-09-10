"""Raw-backed availability, kept separate from metric values and freshness.

Current TikHub adapters establish only available/unavailable, not its cause.
Never translate a missing item, HTTP 404, 401 or 403 into deleted/private.
"""
from __future__ import annotations

import sqlite3

from . import metric_field_facts as facts

DETAIL_OPERATIONS = {"douyin_video_detail", "xiaohongshu_note_detail"}


def record_detail_result(connection: sqlite3.Connection, *, content_id: int,
                         raw_response_id: int, available: bool, recorded_at: str) -> int:
    if not connection.in_transaction or connection.execute("PRAGMA user_version").fetchone()[0] not in {20, 21}:
        raise ValueError("availability requires schema20 writer transaction")
    raw = connection.execute("""SELECT r.*,t.clean_eof,t.json_parse_ok FROM provider_raw_responses r
        JOIN fetch_transport_receipts t ON t.id=r.transport_receipt_id AND t.fetch_attempt_id=r.fetch_attempt_id
        WHERE r.id=? AND r.content_id=?""", (raw_response_id, content_id)).fetchone()
    if raw is None or raw["operation"] not in DETAIL_OPERATIONS or not raw["clean_eof"] or not raw["json_parse_ok"]:
        raise ValueError("availability requires a complete requested-content detail response")
    existing = connection.execute("SELECT id FROM content_availability_observations WHERE content_id=? AND raw_response_id=? AND observation_id IS NULL",
                                  (content_id, raw_response_id)).fetchone()
    if existing is not None:
        return int(existing[0])
    prior = connection.execute("""SELECT * FROM content_availability_observations
        WHERE content_id=? AND provider=? AND operation=? AND availability<>'unknown'
        ORDER BY captured_at DESC,id DESC LIMIT 1""", (content_id, raw["provider"].lower(), raw["operation"])).fetchone()
    if prior is not None and prior["raw_response_id"] == raw_response_id:
        return int(prior["id"])
    state = "available" if available else "unavailable"
    captured = facts.utc(raw["captured_at"])
    confirmed = (prior is not None and prior["availability"] == state and prior["raw_response_id"] != raw_response_id
                 and prior["captured_at"] <= captured)
    reason = "requested_content_available" if available else "confirmed_unavailable" if confirmed else "pending_unavailable_confirmation"
    evidence = {"content_id": content_id, "observation_id": None, "provider": raw["provider"].lower(),
        "operation": raw["operation"], "availability": state, "captured_at": captured,
        "recorded_at": max(captured, facts.utc(recorded_at)), "raw_response_id": raw_response_id, "reason": reason}
    evidence["evidence_sha256"] = facts._hash(evidence)
    return facts._insert(connection, "content_availability_observations", evidence, "evidence_sha256")
