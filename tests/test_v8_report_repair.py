from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import v8.report_repair as repair_module
from v8.report_repair import (
    INVALIDATION_EVENT_TYPE,
    INVALIDATION_REASON,
    ReportRepairError,
    invalidate_unsafe_automatic_reports,
)
from v8.storage import (
    LEGACY_V6_RELEASE_ID,
    LEGACY_V7_RELEASE_ID,
    connect,
    ensure_legacy_evaluation_release,
    initialize_database,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class UnsafeReportRepairTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.db = self.root / "current.sqlite3"
        self.freeze_lock = self.root / "operator-freeze.lock"
        self.freeze_lock.write_text("frozen\n", encoding="utf-8")
        self.artifact_root = self.root / "artifacts"
        self.artifact_root.mkdir()
        self.receipt = self.root / "production-receipt.json"
        self.receipt.write_text("{}\n", encoding="utf-8")
        self.release_manifest = SimpleNamespace(sha256="a" * 64)
        self.load_manifest = patch.object(
            repair_module,
            "_load_freeze_manifest",
            return_value=self.release_manifest,
        ).start()
        self.read_receipt = patch.object(
            repair_module, "_read_receipt", return_value={"receipt": "production"}
        ).start()
        self.require_receipt = patch.object(
            repair_module, "_require_production_receipt_chain"
        ).start()
        self.require_stable = patch.object(
            repair_module, "_attested_activation_stable_state"
        ).start()
        self.addCleanup(patch.stopall)
        self.targets: list[dict[str, object]] = []
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('taxonomy-v5','selling-points-v5.0','published','legacy',
                          '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                """
            )
            ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v6",
                taxonomy_version="selling-points-v5.0",
            )
            ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v7",
                taxonomy_version="selling-points-v5.0",
            )
            for index, task_id in enumerate(
                (
                    "D8-D-20260802-20260802",
                    "D8-D-20260803-20260803",
                    "D8-W-20260727-20260802",
                ),
                1,
            ):
                self.targets.append(
                    self._insert_report(
                        connection,
                        task_id=task_id,
                        task_type="weekly" if index == 3 else "daily",
                        period_start=f"2026-07-{index:02d}",
                        creation_source="automatic",
                        contract_version="dcar-content-operations-report-v8.3",
                        release_id=LEGACY_V7_RELEASE_ID,
                        rule_version="evaluation-v7",
                        taxonomy_version="selling-points-v5.0",
                        scheduler_run_id=index,
                    )
                )
            self._insert_report(
                connection,
                task_id="SAFE-AUTOMATIC-V82",
                task_type="daily",
                period_start="2026-07-10",
                creation_source="automatic",
                contract_version="dcar-content-operations-report-v8.2",
                release_id=LEGACY_V6_RELEASE_ID,
                rule_version="evaluation-v6",
                taxonomy_version="selling-points-v5.0",
            )
            self._insert_report(
                connection,
                task_id="SAFE-MANUAL-V83",
                task_type="custom",
                period_start="2026-07-11",
                creation_source="manual",
                contract_version="dcar-content-operations-report-v8.3",
                release_id=LEGACY_V7_RELEASE_ID,
                rule_version="evaluation-v7",
                taxonomy_version="selling-points-v5.0",
            )
            connection.commit()
            self.frozen_db = self.root / "frozen.sqlite3"
            frozen = sqlite3.connect(self.frozen_db)
            try:
                connection.backup(frozen)
            finally:
                frozen.close()
        self.manifest = self.root / "manifest.json"
        self._write_manifest()
        self.release_manifest.sha256 = _sha256(self.manifest)
        self._activate_v8()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _insert_report(
        self,
        connection: sqlite3.Connection,
        *,
        task_id: str,
        task_type: str,
        period_start: str,
        creation_source: str,
        contract_version: str,
        release_id: str,
        rule_version: str,
        taxonomy_version: str,
        scheduler_run_id: int | None = None,
    ) -> dict[str, object]:
        started_at = "2026-08-04T07:45:50Z"
        completed_at = "2026-08-04T07:45:51Z" if task_type == "weekly" else started_at
        connection.execute(
            """
            INSERT INTO report_tasks(
                id,task_type,name,period_start,period_end,creation_source,
                task_status,progress,message,created_at,started_at,completed_at,updated_at
            ) VALUES (?,?,?,?,?,?,'partial',100,'complete',?,?,?,?)
            """,
            (
                task_id,
                task_type,
                task_id,
                period_start,
                period_start,
                creation_source,
                started_at,
                started_at,
                completed_at,
                completed_at,
            ),
        )
        relative = Path("reports") / task_id / "revision_001" / "report.json"
        artifact = self.artifact_root / relative
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(
            json.dumps({"task_id": task_id}, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        report_sha256 = _sha256(artifact)
        connection.execute(
            """
            INSERT INTO report_revisions(
                task_id,revision,release_id,contract_version,rule_version,
                taxonomy_version,report_json_path,report_sha256,created_at
            ) VALUES (?,1,?,?,?,?,?,?,?)
            """,
            (
                task_id,
                release_id,
                contract_version,
                rule_version,
                taxonomy_version,
                relative.as_posix(),
                report_sha256,
                completed_at,
            ),
        )
        connection.execute(
            """
            INSERT INTO report_files(
                id,task_id,revision,file_kind,local_path,sha256,byte_size,
                status,created_at
            ) VALUES (?, ?,1,'report-json',?,?,?,'available',?)
            """,
            (
                f"file-{task_id}",
                task_id,
                relative.as_posix(),
                report_sha256,
                artifact.stat().st_size,
                completed_at,
            ),
        )
        scheduler_run_ids: list[int] = []
        if scheduler_run_id is not None:
            connection.execute(
                """
                INSERT INTO scheduler_runs(
                    id,job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES (?,? ,?,'succeeded',?,?,?)
                """,
                (
                    scheduler_run_id,
                    f"job-{scheduler_run_id}",
                    f"2026-08-0{scheduler_run_id}T00:00:00Z",
                    started_at,
                    completed_at,
                    json.dumps({"task_id": task_id}),
                ),
            )
            scheduler_run_ids.append(scheduler_run_id)
        return {
            "task_id": task_id,
            "revision": 1,
            "contract_version": contract_version,
            "rule_version": rule_version,
            "taxonomy_version": taxonomy_version,
            "report_json_path": relative.as_posix(),
            "report_sha256": report_sha256,
            "created_at": completed_at,
            "invalidated_at": None,
            "invalidation_reason": None,
            "creation_source": creation_source,
            "task_status": "partial",
            "task_started_at": started_at,
            "task_completed_at": completed_at,
            "scheduler_run_ids": scheduler_run_ids,
        }

    def _write_manifest(self) -> None:
        self.manifest.write_text(
            json.dumps(
                {
                    "schema_version": "dcar-v9-freeze-manifest-v1",
                    "freeze_lock": str(self.freeze_lock),
                    "database_backup": {
                        "path": self.frozen_db.name,
                        "sha256": _sha256(self.frozen_db),
                    },
                    "database_summary": {
                        "unsafe_automatic_report_revisions": self.targets,
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def _activate_v8(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='retired',retired_at='2026-08-04T09:00:00Z',
                    updated_at='2026-08-04T09:00:00Z'
                WHERE id=?
                """,
                (LEGACY_V7_RELEASE_ID,),
            )
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='retired'
                WHERE version='selling-points-v5.0'
                """
            )
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('taxonomy-v51','selling-points-v5.1','published','current',
                          '2026-08-04T09:00:00Z','2026-08-04T09:00:00Z')
                """
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES ('evaluation-v8__selling-points-v5.1','evaluation-v8',
                          'selling-points-v5.1',?,'active',?,?,?)
                """,
                (
                    "8" * 64,
                    "2026-08-04T09:00:00Z",
                    "2026-08-04T09:00:00Z",
                    "2026-08-04T09:00:00Z",
                ),
            )
            connection.commit()

    def _apply(self) -> dict[str, object]:
        return invalidate_unsafe_automatic_reports(
            db_path=self.db,
            manifest_path=self.manifest,
            receipt_path=self.receipt,
            artifact_root=self.artifact_root,
            apply=True,
            acknowledge_rollback_window_close=True,
        )

    def test_dry_run_apply_and_repeat_are_exact_and_preserve_artifacts(self) -> None:
        with connect(self.db) as connection:
            tasks_before = connection.execute(
                """
                SELECT * FROM report_tasks ORDER BY id
                """
            ).fetchall()
            files_before = connection.execute(
                "SELECT * FROM report_files ORDER BY id"
            ).fetchall()
        disk_before = {
            path.relative_to(self.artifact_root): (
                path.read_bytes(),
                path.stat().st_size,
                path.stat().st_mtime_ns,
                _sha256(path),
            )
            for path in self.artifact_root.rglob("*")
            if path.is_file()
        }

        dry_run = invalidate_unsafe_automatic_reports(
            db_path=self.db,
            manifest_path=self.manifest,
            receipt_path=self.receipt,
            artifact_root=self.artifact_root,
        )
        self.assertEqual((dry_run["target_count"], dry_run["pending_count"]), (3, 3))
        self.assertEqual(dry_run["invalidated_count"], 0)
        self.assertEqual(dry_run["verified_file_count"], 3)

        applied = self._apply()
        self.assertEqual(applied["invalidated_count"], 3)
        self.assertEqual(applied["events_inserted"], 3)
        self.assertTrue(applied["rollback_window_closed"])
        with connect(self.db) as connection:
            invalidated = connection.execute(
                """
                SELECT task_id,revision,invalidation_reason FROM report_revisions
                WHERE invalidated_at IS NOT NULL ORDER BY task_id,revision
                """
            ).fetchall()
            events = connection.execute(
                "SELECT * FROM task_events WHERE event_type=? ORDER BY task_id",
                (INVALIDATION_EVENT_TYPE,),
            ).fetchall()
            tasks_after = connection.execute(
                """
                SELECT * FROM report_tasks ORDER BY id
                """
            ).fetchall()
            files_after = connection.execute(
                "SELECT * FROM report_files ORDER BY id"
            ).fetchall()
        self.assertEqual(len(invalidated), 3)
        self.assertTrue(
            all(
                row["invalidation_reason"] == INVALIDATION_REASON for row in invalidated
            )
        )
        self.assertEqual(len(events), 3)
        first_payload = json.loads(events[0]["payload_json"])
        self.assertEqual(first_payload["release_id"], LEGACY_V7_RELEASE_ID)
        self.assertEqual(first_payload["reason"], INVALIDATION_REASON)
        self.assertEqual(
            first_payload["freeze_manifest_sha256"], _sha256(self.manifest)
        )
        self.assertEqual(len(first_payload["scheduler_run_ids"]), 1)
        self.assertEqual(
            [tuple(row) for row in tasks_after], [tuple(row) for row in tasks_before]
        )
        self.assertEqual(
            [tuple(row) for row in files_after], [tuple(row) for row in files_before]
        )
        disk_after = {
            path.relative_to(self.artifact_root): (
                path.read_bytes(),
                path.stat().st_size,
                path.stat().st_mtime_ns,
                _sha256(path),
            )
            for path in self.artifact_root.rglob("*")
            if path.is_file()
        }
        self.assertEqual(disk_after, disk_before)

        with connect(self.db) as connection:
            original_invalidated_at = tuple(
                row[0]
                for row in connection.execute(
                    """
                    SELECT invalidated_at FROM report_revisions
                    WHERE invalidated_at IS NOT NULL ORDER BY task_id
                    """
                )
            )
            connection.execute(
                """
                UPDATE report_tasks
                SET task_status='queued',started_at=NULL,completed_at=NULL,
                    updated_at='2026-08-04T10:00:00Z'
                WHERE id=?
                """,
                (self.targets[0]["task_id"],),
            )
            connection.commit()

        repeated = self._apply()
        self.assertEqual(repeated["invalidated_count"], 0)
        self.assertEqual(repeated["events_inserted"], 0)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM task_events WHERE event_type=?",
                    (INVALIDATION_EVENT_TYPE,),
                ).fetchone()[0],
                3,
            )
            self.assertEqual(
                tuple(
                    row[0]
                    for row in connection.execute(
                        """
                        SELECT invalidated_at FROM report_revisions
                        WHERE invalidated_at IS NOT NULL ORDER BY task_id
                        """
                    )
                ),
                original_invalidated_at,
            )

    def test_apply_requires_explicit_rollback_window_acknowledgement(self) -> None:
        with self.assertRaisesRegex(ReportRepairError, "rollback window"):
            invalidate_unsafe_automatic_reports(
                db_path=self.db,
                manifest_path=self.manifest,
                receipt_path=self.receipt,
                artifact_root=self.artifact_root,
                apply=True,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM report_revisions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )

    def test_any_target_mismatch_rolls_back_the_entire_batch(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE report_revisions SET report_sha256=?
                WHERE task_id=? AND revision=1
                """,
                ("f" * 64, self.targets[-1]["task_id"]),
            )
            connection.commit()
        with self.assertRaisesRegex(ReportRepairError, "no longer matches"):
            self._apply()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM report_revisions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM task_events WHERE event_type=?",
                    (INVALIDATION_EVENT_TYPE,),
                ).fetchone()[0],
                0,
            )

    def test_extra_unsafe_report_not_in_manifest_fails_closed(self) -> None:
        with connect(self.db) as connection:
            self._insert_report(
                connection,
                task_id="EXTRA-UNSAFE",
                task_type="daily",
                period_start="2026-07-20",
                creation_source="automatic",
                contract_version="dcar-content-operations-report-v8.3",
                release_id=LEGACY_V7_RELEASE_ID,
                rule_version="evaluation-v7",
                taxonomy_version="selling-points-v5.0",
            )
            connection.commit()
        with self.assertRaisesRegex(ReportRepairError, "differs from manifest"):
            self._apply()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM report_revisions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )

    def test_frozen_database_hash_mismatch_fails_before_current_write(self) -> None:
        with self.frozen_db.open("ab") as handle:
            handle.write(b"tampered")
        with self.assertRaisesRegex(ReportRepairError, "frozen database hash mismatch"):
            self._apply()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM report_revisions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )

    def test_failure_after_first_update_rolls_back_revisions_and_events(self) -> None:
        def fail_on_second(name: str) -> None:
            if name == "target-2-updated":
                raise RuntimeError("injected failure")

        with (
            patch.object(
                repair_module, "_repair_checkpoint", side_effect=fail_on_second
            ),
            self.assertRaisesRegex(RuntimeError, "injected failure"),
        ):
            self._apply()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM report_revisions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM task_events WHERE event_type=?",
                    (INVALIDATION_EVENT_TYPE,),
                ).fetchone()[0],
                0,
            )

    def test_receipt_mismatch_fails_before_current_write(self) -> None:
        self.require_receipt.side_effect = repair_module.ReleaseManagementError(
            "production receipt belongs to another database"
        )
        with self.assertRaisesRegex(ReportRepairError, "another database"):
            self._apply()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM report_revisions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )

    def test_task_retry_before_initial_invalidation_fails_closed(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE report_tasks
                SET task_status='queued',started_at=NULL,completed_at=NULL,
                    updated_at='2026-08-04T10:00:00Z'
                WHERE id=?
                """,
                (self.targets[0]["task_id"],),
            )
            connection.commit()
        with self.assertRaisesRegex(ReportRepairError, "changed before invalidation"):
            self._apply()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM report_revisions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )

    def test_report_file_row_addition_fails_against_frozen_database(self) -> None:
        target = self.targets[0]
        relative = (
            Path("reports") / str(target["task_id"]) / "revision_001" / "report.md"
        )
        artifact = self.artifact_root / relative
        artifact.write_text("unexpected\n", encoding="utf-8")
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO report_files(
                    id,task_id,revision,file_kind,local_path,sha256,byte_size,
                    status,created_at
                ) VALUES ('unexpected-file',?,1,'report-markdown',?,?,?,
                          'available','2026-08-04T07:45:50Z')
                """,
                (
                    target["task_id"],
                    relative.as_posix(),
                    _sha256(artifact),
                    artifact.stat().st_size,
                ),
            )
            connection.commit()
        with self.assertRaisesRegex(ReportRepairError, "differ from frozen database"):
            self._apply()

    def test_report_artifact_hash_mismatch_fails_without_database_write(self) -> None:
        artifact = self.artifact_root / str(self.targets[0]["report_json_path"])
        artifact.write_text("tampered\n", encoding="utf-8")
        with self.assertRaisesRegex(ReportRepairError, "size mismatch|hash mismatch"):
            self._apply()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM report_revisions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )

    def test_duplicate_manifest_target_is_rejected_before_database_write(self) -> None:
        manifest = json.loads(self.manifest.read_text(encoding="utf-8"))
        manifest["database_summary"]["unsafe_automatic_report_revisions"].append(
            dict(self.targets[0])
        )
        self.manifest.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.release_manifest.sha256 = _sha256(self.manifest)
        with self.assertRaisesRegex(ReportRepairError, "duplicate"):
            self._apply()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM report_revisions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )


if __name__ == "__main__":
    unittest.main()
