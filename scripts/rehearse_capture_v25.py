#!/usr/bin/env python3
"""Read-only SQLite backup and schema20 rehearsal; never installs a candidate."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "dcar_eval"))
from v8 import schema_v20, storage  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new, nonexistent private evidence directory")
    parser.add_argument("--legacy-project-root", type=Path, required=True, help="explicit original root for relative raw evidence paths")
    args = parser.parse_args()
    source = args.source.resolve(strict=True)
    output = args.output.absolute()
    if output.exists() or output.is_symlink():
        raise ValueError("output directory must not already exist")
    output.mkdir(mode=0o700, parents=False)
    candidate = output / "candidate.sqlite3"
    snapshot = output / "source-schema19.sqlite3"
    started = time.monotonic()
    with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as reader, sqlite3.connect(snapshot) as target:
        reader.execute("PRAGMA query_only=ON")
        source_version = reader.execute("PRAGMA user_version").fetchone()[0]
        if source_version != 19:
            raise ValueError("source must be exactly schema19")
        def progress(status: int, remaining: int, total: int) -> None:
            if time.monotonic() - started > 180:
                raise TimeoutError("read-only backup exceeded 180 seconds; source unchanged")
        reader.backup(target, pages=4096, progress=progress, sleep=0.05)
    os.chmod(snapshot, 0o600)
    shutil.copyfile(snapshot, candidate)
    os.chmod(candidate, 0o600)
    print(json.dumps({"stage": "backup_complete", "bytes": candidate.stat().st_size,
                      "seconds": round(time.monotonic() - started, 3)}), flush=True)
    with storage.connect(candidate) as connection:
        receipt = schema_v20.migrate(connection, legacy_project_root=args.legacy_project_root,
                                    migration_blob_root=args.output / "legacy-blobs")
        receipt["integrity_check"] = connection.execute("PRAGMA integrity_check").fetchone()[0]
        receipt["foreign_key_check"] = [tuple(row) for row in connection.execute("PRAGMA foreign_key_check")]
        with sqlite3.connect(snapshot.as_uri() + "?mode=ro", uri=True) as frozen:
            frozen.row_factory = sqlite3.Row
            receipt["lineage"] = schema_v20.validate_lineage(frozen, connection)
        receipt["table_counts"] = {name: connection.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
                                   for name in ("provider_usage", "provider_raw_responses", "content_items",
                                                "content_metric_observations", "content_metric_field_facts",
                                                "provider_usage_settlements", "capture_route_assignments")}
        receipt["source_path"] = str(source)
        receipt["candidate_path"] = str(candidate)
        receipt["snapshot_consistency"] = "sqlite_backup_api"
        receipt["production_mutations"] = 0
        receipt["recorded_at"] = datetime.now(timezone.utc).isoformat()
        receipt["elapsed_seconds"] = round(time.monotonic() - started, 3)
    encoded = json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True).encode()
    path = output / "migration-rehearsal.json"
    with path.open("xb") as stream:
        os.chmod(path, 0o600)
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({"stage": "migration_complete", "receipt": str(path),
                      "receipt_sha256": hashlib.sha256(encoded).hexdigest(),
                      "integrity_check": receipt["integrity_check"], "counts": receipt["table_counts"],
                      "raw": {key: value for key, value in receipt["raw"].items() if not isinstance(value, (list, dict))},
                      "seconds": receipt["elapsed_seconds"]}), flush=True)


if __name__ == "__main__":
    main()
