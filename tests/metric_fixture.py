"""Provider-backed metric fixtures; no production provider calls or inferred provenance."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

from v8.metric_observations import persist_metric_observation


def provider_metric(connection: sqlite3.Connection, **values: Any):
    """Create the raw fixture and immutable observation in the caller transaction."""
    content_id = int(values["content_id"])
    captured_at = str(values["captured_at"])
    provider = values.pop("provider", "tikhub")
    values.pop("source", None)
    values.pop("raw_response_id", None)
    raw_json = json.dumps({"fixture": "metric-v1", "values": values}, sort_keys=True)
    digest = hashlib.sha256(raw_json.encode()).hexdigest()
    canonical_provider = "TikHub" if provider == "tikhub" else "newrank_matrix"
    operation = "douyin_video_detail" if provider == "tikhub" else "matrix_works_list"
    attempt_id = None
    if provider == "tikhub":
        slot = connection.execute(
            """INSERT INTO fetch_slots(
                content_id,stage,window_key,provider,adapter_version,status,
                attempt_count,created_at,updated_at
            ) VALUES (?,'detail',?,'TikHub','fixture-v1','succeeded',1,?,?)""",
            (
                content_id,
                f"metric-fixture:{connection.total_changes}",
                captured_at,
                captured_at,
            ),
        )
        attempt = connection.execute(
            """INSERT INTO fetch_attempts(
                slot_id,attempt_number,request_started_at,response_finished_at,
                http_status,billed
            ) VALUES (?,1,?,?,200,0)""",
            (int(slot.lastrowid), captured_at, captured_at),
        )
        attempt_id = int(attempt.lastrowid)
    raw = connection.execute(
        """INSERT INTO provider_raw_responses(
            fetch_attempt_id,content_id,provider,operation,local_path,sha256,
            byte_size,http_status,captured_at
        ) VALUES (?,?,?,?,?,?,?,200,?)""",
        (attempt_id, content_id, canonical_provider, operation,
         f"fixture-{content_id}-{connection.total_changes}-{digest}.json", digest, len(raw_json.encode()), captured_at),
    )
    values.setdefault("recorded_at", captured_at)
    return persist_metric_observation(connection, **values, provider=provider, raw_response_id=int(raw.lastrowid or 0))
