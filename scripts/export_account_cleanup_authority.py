#!/usr/bin/env python3
"""Export compact immutable source operator authority; no provider or DB writes."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/dcar_eval"))
from v8.account_cleanup_runtime import export_source_authority, file_sha


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-db", "source-sha256", "directory-json", "at", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    connection = sqlite3.connect(Path(args.source_db).resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("BEGIN")
    try:
        value = export_source_authority(connection, json.loads(Path(args.directory_json).read_text()),
                                        source_database_sha256=args.source_sha256, at=args.at)
    finally:
        connection.rollback()
        connection.close()
    path = Path(args.output)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({"path": str(path), "sha256": file_sha(path), "selection_sha256": value["selection_sha256"],
                      "source_members": len(value["source_members"]), "eligible_members": len(value["eligible_members"]),
                      "operations": sorted(value["operations"])}))


if __name__ == "__main__":
    main()
