#!/usr/bin/env python3
"""Real archived response -> schema20 facts -> read-only API, on a rehearsal only.

This verifies real data without buying another request. It is not a live
provider/Writer/publisher rollout and never produces a deployment acceptance.
The rehearsal candidate is changed and is no longer an installable candidate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "dcar_eval"))
from fastapi.testclient import TestClient  # noqa: E402
from v8 import api, metric_field_facts as facts, providers, raw_archive, storage  # noqa: E402
from v8.metric_observations import persist_metric_observation  # noqa: E402


def verify(root: Path) -> dict:
    root = root.resolve(strict=True)
    receipt_path = root / "migration-rehearsal.json"
    receipt = json.loads(receipt_path.read_text())
    db = root / "candidate.sqlite3"
    if db.resolve() == storage.DEFAULT_DB.resolve() or str(db) != receipt["candidate_path"]:
        raise ValueError("only the explicitly recorded rehearsal candidate is accepted")
    source = Path(receipt["source_path"]).resolve(strict=True)
    if os.path.samefile(db, source) or not receipt.get("lineage"):
        raise ValueError("candidate must be separate from source and have verified migration lineage")
    result: dict = {"contract": "capture-v25-real-data-offline-e2e-v1", "samples": [],
        "provider_calls": 0, "production_mutations": 0, "cost_usd": 0,
        "live_provider_e2e": False, "deployment_accepted": False, "candidate_installable_after_replay": False}
    started = time.monotonic()
    with storage.connect(db) as connection:
        if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
            raise ValueError("replay candidate must be schema20")
        before = {table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                  for table in ("provider_usage", "provider_request_start_events", "provider_raw_responses")}
        for platform, operation in (("douyin", "douyin_video_statistics"), ("xiaohongshu", "xiaohongshu_note_detail")):
            candidates = connection.execute("""SELECT r.*,c.platform_content_id,c.content_type,c.link_id
                FROM provider_raw_responses r JOIN content_items c ON c.id=r.content_id
                JOIN fetch_attempts a ON a.id=r.fetch_attempt_id
                WHERE c.platform=? AND lower(r.provider)='tikhub' AND r.operation=? AND r.raw_blob_id IS NOT NULL
                  AND c.content_type='video' AND a.billed=1 AND a.error_code IS NULL
                  ORDER BY r.id DESC LIMIT 20""", (platform,operation)).fetchall()
            selected = None
            rejected = []
            for raw in candidates:
                try:
                    entity = raw_archive.read_response_entity(connection, raw["id"])
                    value = json.loads(entity)
                    if not isinstance(value, dict) or "derived_from_operation" in value or "stage" in value:
                        raise ValueError("offline replay requires an actual provider response, not a derived stage")
                    if platform == "douyin":
                        metrics = providers._parse_douyin_stage_payload("metrics", raw["platform_content_id"], value, status=raw["http_status"] or 200).data
                    else:
                        metrics = providers._parse_xhs_stage_payload("detail", raw["platform_content_id"], "video", value, status=raw["http_status"] or 200).data["metrics"]
                    selected = (raw, entity, metrics)
                    break
                except (ValueError, RuntimeError, KeyError) as error:
                    rejected.append({"raw_response_id": raw["id"], "reason": type(error).__name__})
            if selected is None:
                raise ValueError(f"no replayable {platform} sample among latest twenty: {rejected}")
            raw, entity, metrics = selected
            at = storage.now_utc()
            window = "schema20-offline-e2e:" + str(raw["id"])
            values = {field: metrics.get(field) for field in facts.METRIC_FIELDS}
            arguments = {"content_id": raw["content_id"], "captured_at": raw["captured_at"], "window_key": window,
                **values, "status": "available", "source": "tikhub", "provider": "tikhub", "platform": platform,
                "raw_response_id": raw["id"], "metadata_json": json.dumps({"operation": operation, "fields": metrics["_field_status"]}),
                "recorded_at": at}
            with storage.transaction(connection):
                saved = persist_metric_observation(connection, **arguments)
                projected = facts.project_content(connection, raw["content_id"], cutoff_at=at, knowledge_at=at, window_key=window)
                replayed = persist_metric_observation(connection, **arguments)
                if replayed.observation_created or replayed.observation_id != saved.observation_id:
                    raise AssertionError("raw replay created a duplicate observation")
            fact_count = connection.execute("SELECT count(*) FROM content_metric_field_facts WHERE observation_id=?", (saved.observation_id,)).fetchone()[0]
            if fact_count != 5:
                raise AssertionError("one observation must have five explicit field facts")
            for fact in connection.execute("SELECT field,observed_value_json FROM content_metric_field_facts WHERE observation_id=?", (saved.observation_id,)):
                if json.loads(fact["observed_value_json"]) != values[fact["field"]]:
                    raise AssertionError("stored field differs from the real raw parser output")
            result["samples"].append({"platform": platform, "content_id": raw["content_id"], "link_id": raw["link_id"],
                "raw_response_id": raw["id"], "entity_sha256": hashlib.sha256(entity).hexdigest(), "entity_bytes": len(entity),
                "captured_at": raw["captured_at"], "recorded_at": at, "observation_id": saved.observation_id,
                "field_facts": fact_count, "projection_id": projected["projection_id"], "replay_idempotent": True,
                "skipped_unusable_raws": rejected})
        after = {table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in before}
        if before != after:
            raise AssertionError("offline materialization changed provider/network/raw ledgers")
        result["unchanged_provider_ledgers"] = after
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    config = api.ApiConfig(db_path=db, reports_root=root/"reports", legacy_db_path=root/"unused-legacy.sqlite3",
        operator_freeze_lock=root/"unused-freeze.lock", writer_lock=root/"unused-writer.lock", read_only=True,
        scheduler_enabled=False, startup_catchup_enabled=False, project_root=root)
    with TestClient(api.create_app(config)) as client:
        for sample in result["samples"]:
            response = client.post("/api/v8/contents/search", json={"query": sample["link_id"], "platform": sample["platform"]})
            if response.status_code != 200:
                raise AssertionError(f"read-only API failed with HTTP {response.status_code}")
            body = response.json()
            matching = [item for item in body.get("items", []) if item.get("id") == sample["content_id"]
                        and item.get("platform") == sample["platform"] and item.get("link_id") == sample["link_id"]]
            if len(matching) != 1:
                raise AssertionError("requested real content was absent from API")
            with storage.connect(db, read_only=True) as connection:
                selected = facts.select_field_facts(connection, sample["content_id"], cutoff_at=storage.now_utc())
                canonical = facts.business_projection(connection, selected)
            if canonical is None:
                raise AssertionError("real content has no canonical projection")
            if any(matching[0].get(field) != canonical.get(field) for field in facts.METRIC_FIELDS):
                raise AssertionError("API values differ from canonical schema20 fields")
            sample["api_status"] = response.status_code
            sample["api_values_match_canonical"] = True
        ready = client.get("/api/v8/readyz")
        result["readiness_http_status"] = ready.status_code
    with sqlite3.connect(source.as_uri()+"?mode=ro", uri=True) as reader:
        result["source_schema_after"] = reader.execute("PRAGMA user_version").fetchone()[0]
    result["elapsed_seconds"] = round(time.monotonic()-started,3)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rehearsal-root", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.rehearsal_root)
    path = args.rehearsal_root / "real-data-replay.json"
    data = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True).encode()
    with path.open("xb") as output:
        os.chmod(path,0o600)
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    print(json.dumps({"receipt":str(path), "sha256":hashlib.sha256(data).hexdigest(), **result},ensure_ascii=False))


if __name__ == "__main__":
    main()
