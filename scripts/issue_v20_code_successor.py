#!/usr/bin/env python3
"""Offline, Writer-owned issuer for explicitly reviewed code successors.

prepare -> ordinary explicit sealer --code-successor-plan -> postseal.
Neither action changes accepted/activation/paid gates or migrates the database.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "postseal"))
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--previous-build", type=Path)
    parser.add_argument("--build", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--test-result", action="append", default=[])
    parser.add_argument("--actor")
    parser.add_argument("--reason")
    parser.add_argument("--transition", choices=("account-workbench-20260907-v1", "writer-source-isolation-20260907-v1"))
    args = parser.parse_args()
    project = args.project_root.resolve(strict=True)
    source = Path(__file__).resolve().parents[1]
    sys.path[:0] = [str(source / "scripts"), str(source / "src/dcar_eval")]
    import seal_r0_receipts as sealer
    from v8 import capture_code_successor as successor
    from v8.runtime_database import (
        DatabaseAccessMode, acquire_writer_lock, load_installed_writer_contract,
        resolve_installed_database_access,
    )
    from v8.storage import now_utc

    installed = load_installed_writer_contract(required=True)
    assert installed is not None
    access = resolve_installed_database_access(
        DatabaseAccessMode.WRITER, database=installed.database, project_root=project,
        environ={"DCAR_PROJECT_ROOT": str(installed.project_root),
            "DCAR_V8_DB": str(installed.database), "DCAR_WRITER_LOCK": str(installed.writer_lock)},
        installed=installed,
    )
    with acquire_writer_lock(access):
        # The runtime registry must own this exact Writer lease. A sealer's
        # external maintenance lease is not authority to append control rows.
        sealer._require_no_database_holders(installed.database)
        with closing(sqlite3.connect(f"{installed.database.as_uri()}?mode=rw", uri=True)) as connection, connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA recursive_triggers=ON")
            connection.execute("BEGIN IMMEDIATE")
            at = now_utc()
            if args.action == "prepare":
                if not all((args.previous_build, args.evidence_dir, args.actor, args.reason, args.test_result)):
                    parser.error("prepare requires previous-build, evidence-dir, actor, reason and test-result")
                result = successor.prepare_plan(connection, project_root=project, previous_build=args.previous_build,
                    evidence_dir=args.evidence_dir, tests=sealer._parse_test_results(args.test_result),
                    actor=args.actor, reason=args.reason, at=at, transition=args.transition)
            else:
                if args.build is None:
                    parser.error("postseal requires build")
                result = successor.issue_decision(connection, project_root=project, build_path=args.build,
                    mirror_root=installed.database.parent / "current-hold-control", at=at)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
