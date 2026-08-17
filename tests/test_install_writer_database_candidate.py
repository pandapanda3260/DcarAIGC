from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

from v8.release_management_v9 import (
    REPORT_VERSION,
    SOURCE_RELEASE_ID,
    SOURCE_RULE_VERSION,
    TAXONOMY_VERSION,
)
from v8.storage import connect, initialize_database, now_utc


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "install_writer_database_candidate.py"
SPEC = importlib.util.spec_from_file_location(
    "install_writer_database_candidate", SCRIPT
)
assert SPEC is not None and SPEC.loader is not None
installer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = installer
SPEC.loader.exec_module(installer)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class WriterDatabaseCandidateInstallerTest(unittest.TestCase):
    def _build_source_database(
        self,
        path: Path,
    ) -> None:
        captured_at = now_utc()
        connection = connect(path)
        try:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,source_path,source_sha256,
                    created_at,published_at
                ) VALUES ('candidate-taxonomy',?,'published','fixture',NULL,NULL,?,?)
                """,
                (TAXONOMY_VERSION, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES (?,?,? ,?,'active',?,?,?)
                """,
                (
                    SOURCE_RELEASE_ID,
                    SOURCE_RULE_VERSION,
                    TAXONOMY_VERSION,
                    "a" * 64,
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id,platform,platform_content_id,canonical_url,title,body,
                    content_type,published_at,source_group,source_label,source_path,
                    imported_at,created_at,updated_at
                ) VALUES (
                    'ABC123','douyin','fixture-content','https://example.test/fixture',
                    'fixture','fixture','video','2026-08-04T00:00:00Z','','fixture',
                    'fixture.json',?,?,?
                )
                """,
                (captured_at, captured_at, captured_at),
            )
            content_id = int(
                connection.execute("SELECT last_insert_rowid()").fetchone()[0]
            )
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id,captured_at,window_key,view_count,comment_count,
                    like_count,share_count,collect_count,status,source,metadata_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    content_id,
                    "2026-08-05T00:00:00Z",
                    "fixture-window",
                    10,
                    2,
                    3,
                    4,
                    5,
                    "available",
                    "fixture",
                    '{"fixture":true}',
                ),
            )
            connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES (?,?,?,?,?,?)
                """,
                (
                    "daily_capture",
                    "2026-08-05T18:00:00Z",
                    "succeeded",
                    "2026-08-05T18:00:00Z",
                    "2026-08-05T18:01:00Z",
                    '{"fixture":true}',
                ),
            )
            connection.commit()
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            connection.close()

        # Fresh databases use the current schema. Rebuild the two changed
        # surfaces into their exact v11 form so the test exercises the real
        # v11 -> v12 -> v13 migration rather than two independently built v13s.
        raw = sqlite3.connect(path)
        try:
            raw.execute("PRAGMA foreign_keys=OFF")
            raw.executescript(
                """
                DROP TRIGGER trg_scheduler_run_attempts_terminal_update;
                DROP TRIGGER trg_scheduler_run_attempts_no_delete;
                DROP INDEX uq_scheduler_run_attempts_active;
                DROP TABLE scheduler_run_attempts;
                ALTER TABLE scheduler_runs RENAME TO scheduler_runs_v13_fixture;
                CREATE TABLE scheduler_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    scheduled_for TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed','skipped')),
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    details_json TEXT NOT NULL DEFAULT '{}',
                    UNIQUE(job_id, scheduled_for)
                );
                INSERT INTO scheduler_runs(
                    id,job_id,scheduled_for,status,started_at,completed_at,details_json
                )
                SELECT id,job_id,scheduled_for,status,started_at,completed_at,details_json
                FROM scheduler_runs_v13_fixture;
                DROP TABLE scheduler_runs_v13_fixture;
                DROP TRIGGER trg_metric_observations_immutable_payload;
                DROP TRIGGER trg_metric_observations_no_delete;
                DROP INDEX idx_metric_observations_content_capture;
                DROP TABLE content_metric_observations;
                DROP INDEX idx_content_identities_content_primary;
                DELETE FROM schema_migrations WHERE version >= 12;
                PRAGMA user_version=11;
                """
            )
            raw.commit()
        finally:
            raw.close()

    def _build_candidate_from_source(self, source: Path, candidate: Path) -> None:
        shutil.copy2(source, candidate)
        connection = connect(candidate)
        try:
            initialize_database(connection)
            connection.commit()
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.execute("PRAGMA journal_mode=DELETE")
        finally:
            connection.close()

    def _layout(self, root: Path, *, sidecars: bool = False) -> dict[str, Path]:
        data = root / "app" / "data"
        backups = data / "backups"
        candidate_root = root / "candidate"
        receipt_root = root / "receipts"
        runtime = root / "runtime"
        for directory in (backups, candidate_root, receipt_root, runtime):
            directory.mkdir(parents=True, exist_ok=True)
        formal = data / "dcar_insight.sqlite3"
        candidate = candidate_root / "candidate.sqlite3"
        lock = runtime / "operator-freeze.lock"
        lock.write_text("frozen\n", encoding="utf-8")
        self._build_source_database(formal)
        self._build_candidate_from_source(formal, candidate)
        if sidecars:
            Path(f"{formal}-wal").write_bytes(b"")
            Path(f"{formal}-shm").write_bytes(b"preserved old shm")
        return {
            "formal": formal,
            "candidate": candidate,
            "backups": backups,
            "backup": backups / "install-001",
            "receipt": receipt_root / "install-001.json",
            "lock": lock,
        }

    def _install_arguments(self, layout: dict[str, Path]) -> dict[str, Any]:
        return {
            "formal_database": layout["formal"],
            "candidate": layout["candidate"],
            "expected_source_sha256": _sha256(layout["formal"]),
            "expected_candidate_sha256": _sha256(layout["candidate"]),
            "backup_directory": layout["backup"],
            "receipt": layout["receipt"],
            "freeze_lock": layout["lock"],
        }

    def _constant_patches(self, layout: dict[str, Path]) -> tuple[Any, Any, Any]:
        return (
            patch.object(installer, "FORMAL_DATABASE", layout["formal"]),
            patch.object(installer, "FORMAL_BACKUP_ROOT", layout["backups"]),
            patch.multiple(
                installer,
                CANONICAL_OPERATOR_FREEZE_LOCK=layout["lock"],
                EXPECTED_SOURCE_CONTENT_COUNT=1,
                EXPECTED_SOURCE_METRIC_SNAPSHOT_COUNT=1,
                EXPECTED_SOURCE_SCHEDULER_RUN_COUNT=1,
            ),
        )

    def test_success_atomically_preserves_database_and_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            arguments = self._install_arguments(layout)
            source_sha = str(arguments["expected_source_sha256"])
            candidate_sha = str(arguments["expected_candidate_sha256"])
            wal_sha = _sha256(Path(f"{layout['formal']}-wal"))
            shm_sha = _sha256(Path(f"{layout['formal']}-shm"))
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_writer_handles", return_value=[]),
            ):
                result = installer.install_candidate(**arguments)

            self.assertEqual(result["status"], "installed")
            self.assertEqual(_sha256(layout["formal"]), candidate_sha)
            self.assertFalse(layout["candidate"].exists())
            backup_database = layout["backup"] / layout["formal"].name
            self.assertEqual(_sha256(backup_database), source_sha)
            self.assertEqual(
                _sha256(layout["backup"] / f"{layout['formal'].name}-wal"),
                wal_sha,
            )
            self.assertEqual(
                _sha256(layout["backup"] / f"{layout['formal'].name}-shm"),
                shm_sha,
            )
            self.assertFalse(Path(f"{layout['formal']}-wal").exists())
            self.assertFalse(Path(f"{layout['formal']}-shm").exists())
            self.assertEqual(layout["formal"].stat().st_mode & 0o777, 0o600)
            receipt = json.loads(layout["receipt"].read_text(encoding="utf-8"))
            self.assertEqual(receipt, result)
            self.assertEqual(receipt["before"]["database"]["sha256"], source_sha)
            self.assertEqual(receipt["installed"]["file"]["sha256"], candidate_sha)
            self.assertEqual(receipt["backup"]["database"]["sha256"], source_sha)
            self.assertEqual(receipt["candidate"]["validation"]["schema_version"], 13)
            self.assertEqual(
                receipt["candidate"]["validation"]["v8_6_report_revision_count"],
                0,
            )

    def test_every_durable_checkpoint_rolls_back_database_and_sidecars(self) -> None:
        for checkpoint in installer.INSTALL_CHECKPOINTS:
            with (
                self.subTest(checkpoint=checkpoint),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary), sidecars=True)
                arguments = self._install_arguments(layout)
                source_sha = str(arguments["expected_source_sha256"])
                candidate_sha = str(arguments["expected_candidate_sha256"])
                wal_path = Path(f"{layout['formal']}-wal")
                shm_path = Path(f"{layout['formal']}-shm")
                wal_sha = _sha256(wal_path)
                shm_sha = _sha256(shm_path)

                def fail_at(name: str) -> None:
                    if name == checkpoint:
                        raise RuntimeError(f"injected failure at {name}")

                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    patch.object(
                        installer, "_database_writer_handles", return_value=[]
                    ),
                    self.assertRaisesRegex(
                        installer.CandidateInstallError, "rolled back"
                    ),
                ):
                    installer.install_candidate(**arguments, fault_injector=fail_at)

                self.assertEqual(_sha256(layout["formal"]), source_sha)
                self.assertEqual(_sha256(layout["candidate"]), candidate_sha)
                self.assertEqual(_sha256(wal_path), wal_sha)
                self.assertEqual(_sha256(shm_path), shm_sha)
                self.assertFalse(layout["receipt"].exists())
                if layout["backup"].exists():
                    marker = json.loads(
                        (layout["backup"] / "FAILED.json").read_text(encoding="utf-8")
                    )
                    self.assertEqual(marker["status"], "rolled_back")
                    self.assertEqual(
                        marker["candidate_trace_path"], str(layout["candidate"])
                    )

    def test_rollback_quarantines_new_candidate_sidecars_before_restoring_source(
        self,
    ) -> None:
        for original_sidecars in (False, True):
            with (
                self.subTest(original_sidecars=original_sidecars),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary), sidecars=original_sidecars)
                arguments = self._install_arguments(layout)
                source_sha = str(arguments["expected_source_sha256"])
                candidate_sha = str(arguments["expected_candidate_sha256"])
                original_wal_sha = (
                    _sha256(Path(f"{layout['formal']}-wal"))
                    if original_sidecars
                    else None
                )
                original_shm_sha = (
                    _sha256(Path(f"{layout['formal']}-shm"))
                    if original_sidecars
                    else None
                )

                def create_candidate_sidecars(name: str) -> None:
                    if name == "after_installed_file_synced":
                        Path(f"{layout['formal']}-wal").write_bytes(
                            b"new candidate wal"
                        )
                        Path(f"{layout['formal']}-shm").write_bytes(
                            b"new candidate shm"
                        )
                        raise RuntimeError("injected candidate sidecars")

                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    patch.object(
                        installer, "_database_writer_handles", return_value=[]
                    ),
                    self.assertRaisesRegex(
                        installer.CandidateInstallError, "rolled back"
                    ),
                ):
                    installer.install_candidate(
                        **arguments, fault_injector=create_candidate_sidecars
                    )

                self.assertEqual(_sha256(layout["formal"]), source_sha)
                self.assertEqual(_sha256(layout["candidate"]), candidate_sha)
                for suffix, expected_payload in (
                    ("-wal", b"new candidate wal"),
                    ("-shm", b"new candidate shm"),
                ):
                    quarantined = (
                        layout["backup"]
                        / f"FAILED-installed-{layout['formal'].name}{suffix}"
                    )
                    self.assertEqual(quarantined.read_bytes(), expected_payload)
                if original_sidecars:
                    self.assertEqual(
                        _sha256(Path(f"{layout['formal']}-wal")),
                        original_wal_sha,
                    )
                    self.assertEqual(
                        _sha256(Path(f"{layout['formal']}-shm")),
                        original_shm_sha,
                    )
                else:
                    self.assertFalse(Path(f"{layout['formal']}-wal").exists())
                    self.assertFalse(Path(f"{layout['formal']}-shm").exists())
                marker = json.loads(
                    (layout["backup"] / "FAILED.json").read_text(encoding="utf-8")
                )
                self.assertEqual(marker["status"], "rolled_back")
                self.assertEqual(len(marker["quarantined_paths"]), 2)

    def test_active_writer_during_rollback_fails_closed_without_mixing_files(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            arguments = self._install_arguments(layout)
            source_sha = str(arguments["expected_source_sha256"])
            candidate_sha = str(arguments["expected_candidate_sha256"])
            calls = 0

            def writer_handles(_databases: Any) -> list[dict[str, Any]]:
                nonlocal calls
                calls += 1
                if calls < 4:
                    return []
                return [
                    {
                        "command": "python",
                        "pid": 999,
                        "descriptor": "4uW",
                        "path": str(layout["formal"]),
                    }
                ]

            def fail_after_install(name: str) -> None:
                if name == "after_installed_file_synced":
                    Path(f"{layout['formal']}-wal").write_bytes(b"writer wal")
                    raise RuntimeError("writer raced installation")

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(
                    installer,
                    "_database_writer_handles",
                    side_effect=writer_handles,
                ),
                self.assertRaisesRegex(
                    installer.CandidateInstallError, "rollback was incomplete"
                ),
            ):
                installer.install_candidate(
                    **arguments, fault_injector=fail_after_install
                )

            self.assertEqual(_sha256(layout["formal"]), candidate_sha)
            self.assertFalse(layout["candidate"].exists())
            self.assertEqual(
                _sha256(layout["backup"] / layout["formal"].name), source_sha
            )
            self.assertEqual(
                Path(f"{layout['formal']}-wal").read_bytes(), b"writer wal"
            )
            marker = json.loads(
                (layout["backup"] / "FAILED.json").read_text(encoding="utf-8")
            )
            self.assertEqual(marker["status"], "rollback_incomplete")

    def test_racing_formal_path_is_preserved_without_clobbering_and_source_returns(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            arguments = self._install_arguments(layout)
            source_sha = str(arguments["expected_source_sha256"])
            candidate_sha = str(arguments["expected_candidate_sha256"])

            def recreate_formal(name: str) -> None:
                if name == "after_source_shm_moved":
                    layout["formal"].write_bytes(b"racing database")

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_writer_handles", return_value=[]),
                self.assertRaisesRegex(installer.CandidateInstallError, "rolled back"),
            ):
                installer.install_candidate(**arguments, fault_injector=recreate_formal)

            self.assertEqual(_sha256(layout["formal"]), source_sha)
            self.assertEqual(_sha256(layout["candidate"]), candidate_sha)
            self.assertEqual(
                (
                    layout["backup"] / f"FAILED-racing-{layout['formal'].name}"
                ).read_bytes(),
                b"racing database",
            )

    def test_receipt_fsync_failure_removes_installer_owned_receipt_and_rolls_back(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            arguments = self._install_arguments(layout)
            source_sha = str(arguments["expected_source_sha256"])
            candidate_sha = str(arguments["expected_candidate_sha256"])
            original_fsync = installer._fsync_directory
            failed = False

            def fail_receipt_parent_once(path: Path) -> None:
                nonlocal failed
                if path == layout["receipt"].parent and not failed:
                    failed = True
                    raise OSError("injected receipt parent fsync failure")
                original_fsync(path)

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_writer_handles", return_value=[]),
                patch.object(
                    installer,
                    "_fsync_directory",
                    side_effect=fail_receipt_parent_once,
                ),
                self.assertRaisesRegex(installer.CandidateInstallError, "rolled back"),
            ):
                installer.install_candidate(**arguments)

            self.assertTrue(failed)
            self.assertFalse(layout["receipt"].exists())
            self.assertEqual(_sha256(layout["formal"]), source_sha)
            self.assertEqual(_sha256(layout["candidate"]), candidate_sha)

    def test_refuses_writer_handle_before_creating_backup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            arguments = self._install_arguments(layout)
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(
                    installer,
                    "_database_writer_handles",
                    return_value=[
                        {
                            "command": "python",
                            "pid": 123,
                            "descriptor": "4u",
                            "path": str(layout["formal"]),
                        }
                    ],
                ),
                self.assertRaisesRegex(
                    installer.CandidateInstallError, "writer handles"
                ),
            ):
                installer.install_candidate(**arguments)
            self.assertFalse(layout["backup"].exists())
            self.assertTrue(layout["formal"].exists())
            self.assertTrue(layout["candidate"].exists())

    def test_refuses_source_and_candidate_sha_mismatches(self) -> None:
        for field in ("expected_source_sha256", "expected_candidate_sha256"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                layout = self._layout(Path(temporary))
                arguments = self._install_arguments(layout)
                arguments[field] = "f" * 64
                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    patch.object(
                        installer, "_database_writer_handles", return_value=[]
                    ),
                    self.assertRaisesRegex(installer.CandidateInstallError, "SHA-256"),
                ):
                    installer.install_candidate(**arguments)
                self.assertFalse(layout["backup"].exists())

    def test_refuses_candidate_with_missing_or_mutated_preexisting_business_data(
        self,
    ) -> None:
        mutations = (
            (
                "mutated-content",
                "UPDATE content_items SET title='silently changed'",
                "content_items",
            ),
            (
                "missing-snapshot",
                "DELETE FROM content_metric_snapshots",
                "content_metric_snapshots",
            ),
        )
        for name, statement, expected_table in mutations:
            with (
                self.subTest(case=name),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary))
                raw = sqlite3.connect(layout["candidate"])
                try:
                    raw.execute(statement)
                    raw.commit()
                finally:
                    raw.close()
                arguments = self._install_arguments(layout)
                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    patch.object(
                        installer, "_database_writer_handles", return_value=[]
                    ),
                    self.assertRaisesRegex(
                        installer.CandidateInstallError, expected_table
                    ),
                ):
                    installer.install_candidate(**arguments)
                self.assertFalse(layout["backup"].exists())

    def test_refuses_candidate_that_mutates_existing_migration_or_sequence(
        self,
    ) -> None:
        mutations = (
            (
                "migration",
                "UPDATE schema_migrations SET applied_at='changed' WHERE version=11",
                "pre-existing schema migration",
            ),
            (
                "sequence",
                "UPDATE sqlite_sequence SET seq=seq+10 WHERE name='content_items'",
                "AUTOINCREMENT sequences",
            ),
        )
        for name, statement, message in mutations:
            with (
                self.subTest(case=name),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary))
                raw = sqlite3.connect(layout["candidate"])
                try:
                    raw.execute(statement)
                    raw.commit()
                finally:
                    raw.close()
                arguments = self._install_arguments(layout)
                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    patch.object(
                        installer, "_database_writer_handles", return_value=[]
                    ),
                    self.assertRaisesRegex(installer.CandidateInstallError, message),
                ):
                    installer.install_candidate(**arguments)
                self.assertFalse(layout["backup"].exists())

    def test_refuses_field_tampering_in_metric_and_scheduler_migration_rows(
        self,
    ) -> None:
        cases = (
            (
                "metric-observation",
                "trg_metric_observations_immutable_payload",
                "UPDATE content_metric_observations "
                "SET subject_key='wrong-subject',observation_sha256='f' || "
                "substr(observation_sha256,2)",
                "metric observation projection",
            ),
            (
                "scheduler-attempt",
                "trg_scheduler_run_attempts_terminal_update",
                "UPDATE scheduler_run_attempts SET details_json='{\"wrong\":true}'",
                "scheduler attempt baseline",
            ),
        )
        for name, trigger, statement, message in cases:
            with (
                self.subTest(case=name),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary))
                raw = sqlite3.connect(layout["candidate"])
                try:
                    trigger_sql = str(
                        raw.execute(
                            "SELECT sql FROM sqlite_master "
                            "WHERE type='trigger' AND name=?",
                            (trigger,),
                        ).fetchone()[0]
                    )
                    raw.execute(f'DROP TRIGGER "{trigger}"')
                    raw.execute(statement)
                    raw.execute(trigger_sql)
                    raw.commit()
                finally:
                    raw.close()
                arguments = self._install_arguments(layout)
                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    patch.object(
                        installer, "_database_writer_handles", return_value=[]
                    ),
                    self.assertRaisesRegex(installer.CandidateInstallError, message),
                ):
                    installer.install_candidate(**arguments)
                self.assertFalse(layout["backup"].exists())

    def test_refuses_exact_source_count_drift_even_if_candidate_matches(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            for database, statement in (
                (layout["formal"], "DELETE FROM content_metric_snapshots"),
                (layout["candidate"], "DELETE FROM content_metric_snapshots"),
            ):
                raw = sqlite3.connect(database)
                try:
                    raw.execute(statement)
                    raw.commit()
                finally:
                    raw.close()
            raw = sqlite3.connect(layout["candidate"])
            try:
                trigger = "trg_metric_observations_no_delete"
                trigger_sql = str(
                    raw.execute(
                        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
                        (trigger,),
                    ).fetchone()[0]
                )
                raw.execute(f'DROP TRIGGER "{trigger}"')
                raw.execute("DELETE FROM content_metric_observations")
                raw.execute(trigger_sql)
                raw.commit()
            finally:
                raw.close()
            arguments = self._install_arguments(layout)
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_writer_handles", return_value=[]),
                self.assertRaisesRegex(
                    installer.CandidateInstallError, "metric snapshot count changed"
                ),
            ):
                installer.install_candidate(**arguments)
            self.assertFalse(layout["backup"].exists())

    def test_refuses_candidate_identity_and_cutover_state_errors(self) -> None:
        cases = (
            (
                "wrong-active",
                "evaluation-v7__selling-points-v5.2",
                "evaluation-v7",
                False,
                "active evaluation-v8",
            ),
            (
                "existing-v8.6",
                SOURCE_RELEASE_ID,
                SOURCE_RULE_VERSION,
                True,
                "zero v8.6",
            ),
        )
        for name, release_id, rule_version, with_revision, message in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as temporary:
                layout = self._layout(Path(temporary))
                raw = sqlite3.connect(layout["candidate"])
                try:
                    if release_id != SOURCE_RELEASE_ID:
                        raw.execute(
                            """
                            UPDATE evaluation_releases
                            SET id=?,rule_version=? WHERE id=?
                            """,
                            (release_id, rule_version, SOURCE_RELEASE_ID),
                        )
                    if with_revision:
                        captured_at = now_utc()
                        raw.execute(
                            """
                            INSERT INTO report_tasks(
                                id,task_type,name,period_start,period_end,
                                creation_source,task_status,progress,created_at,updated_at
                            ) VALUES (
                                'precutover-report','daily','precutover','2026-08-04',
                                '2026-08-04','manual','partial',100,?,?
                            )
                            """,
                            (captured_at, captured_at),
                        )
                        raw.execute(
                            """
                            INSERT INTO report_revisions(
                                task_id,revision,release_id,contract_version,
                                rule_version,taxonomy_version,report_json_path,
                                report_sha256,created_at
                            ) VALUES ('precutover-report',1,?,?,?,?,?,?,?)
                            """,
                            (
                                release_id,
                                REPORT_VERSION,
                                rule_version,
                                TAXONOMY_VERSION,
                                "report.json",
                                "b" * 64,
                                captured_at,
                            ),
                        )
                    raw.commit()
                finally:
                    raw.close()
                arguments = self._install_arguments(layout)
                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    patch.object(
                        installer, "_database_writer_handles", return_value=[]
                    ),
                    self.assertRaisesRegex(installer.CandidateInstallError, message),
                ):
                    installer.install_candidate(**arguments)
                self.assertFalse(layout["backup"].exists())

    def test_refuses_inexact_candidate_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            raw = sqlite3.connect(layout["candidate"])
            raw.execute("PRAGMA user_version=12")
            raw.commit()
            raw.close()
            arguments = self._install_arguments(layout)
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_writer_handles", return_value=[]),
                self.assertRaisesRegex(installer.CandidateInstallError, "schema"),
            ):
                installer.install_candidate(**arguments)

    def test_refuses_every_candidate_sidecar_and_nonempty_formal_wal(self) -> None:
        for suffix in installer.SQLITE_TRANSIENT_SUFFIXES:
            with (
                self.subTest(candidate_suffix=suffix),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary))
                Path(f"{layout['candidate']}{suffix}").write_bytes(b"sidecar")
                arguments = self._install_arguments(layout)
                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    self.assertRaisesRegex(
                        installer.CandidateInstallError, "self-contained"
                    ),
                ):
                    installer.install_candidate(**arguments)
                self.assertFalse(layout["backup"].exists())

        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            Path(f"{layout['formal']}-wal").write_bytes(b"uncheckpointed")
            arguments = self._install_arguments(layout)
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_writer_handles", return_value=[]),
                self.assertRaisesRegex(
                    installer.CandidateInstallError, "formal source WAL"
                ),
            ):
                installer.install_candidate(**arguments)
            self.assertFalse(layout["backup"].exists())

    def test_refuses_symlinks_hardlinks_and_same_inode(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            real_candidate = layout["candidate"]
            symlink = real_candidate.with_name("candidate-link.sqlite3")
            symlink.symlink_to(real_candidate)
            arguments = self._install_arguments(layout)
            arguments["candidate"] = symlink
            arguments["expected_candidate_sha256"] = _sha256(real_candidate)
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                self.assertRaisesRegex(installer.CandidateInstallError, "non-symlink"),
            ):
                installer.install_candidate(**arguments)

        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            extra_link = layout["candidate"].with_name("candidate-hardlink.sqlite3")
            os.link(layout["candidate"], extra_link)
            arguments = self._install_arguments(layout)
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                self.assertRaisesRegex(installer.CandidateInstallError, "hard-linked"),
            ):
                installer.install_candidate(**arguments)

        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            layout["candidate"].unlink()
            os.link(layout["formal"], layout["candidate"])
            arguments = self._install_arguments(layout)
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                self.assertRaisesRegex(
                    installer.CandidateInstallError, "hard-linked|different inodes"
                ),
            ):
                installer.install_candidate(**arguments)

    def test_refuses_cross_device_candidate_before_any_move(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            arguments = self._install_arguments(layout)
            original = installer._stat_identity

            def cross_device(path: Path) -> Any:
                identity = original(path)
                if path == layout["candidate"]:
                    return replace(identity, device=identity.device + 1)
                return identity

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_stat_identity", side_effect=cross_device),
                self.assertRaisesRegex(
                    installer.CandidateInstallError, "same filesystem"
                ),
            ):
                installer.install_candidate(**arguments)
            self.assertFalse(layout["backup"].exists())

    def test_refuses_path_escape_existing_outputs_and_noncanonical_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            cases = (
                (
                    "backup_directory",
                    layout["backups"].parent / "escaped",
                    "direct child",
                ),
                ("receipt", layout["backups"] / "receipt.json", "outside"),
                (
                    "backup_directory",
                    layout["backups"] / "nested" / "install",
                    "direct child",
                ),
            )
            for field, value, message in cases:
                with self.subTest(field=field):
                    arguments = self._install_arguments(layout)
                    arguments[field] = value
                    first, second, third = self._constant_patches(layout)
                    with (
                        first,
                        second,
                        third,
                        self.assertRaisesRegex(
                            installer.CandidateInstallError, message
                        ),
                    ):
                        installer.install_candidate(**arguments)

            alternate = layout["lock"].with_name("alternate.lock")
            alternate.write_text("frozen\n", encoding="utf-8")
            arguments = self._install_arguments(layout)
            arguments["freeze_lock"] = alternate
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                self.assertRaisesRegex(installer.CandidateInstallError, "canonical"),
            ):
                installer.install_candidate(**arguments)

    def test_receipt_is_exclusive_and_preexisting_path_blocks_install(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            layout["receipt"].write_text("do not overwrite\n", encoding="utf-8")
            arguments = self._install_arguments(layout)
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                self.assertRaisesRegex(
                    installer.CandidateInstallError, "must not already exist"
                ),
            ):
                installer.install_candidate(**arguments)
            self.assertEqual(
                layout["receipt"].read_text(encoding="utf-8"), "do not overwrite\n"
            )
            self.assertFalse(layout["backup"].exists())

    def test_receipt_o_excl_race_rolls_back_without_overwriting_racer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            arguments = self._install_arguments(layout)
            source_sha = str(arguments["expected_source_sha256"])
            candidate_sha = str(arguments["expected_candidate_sha256"])

            def create_racing_receipt(name: str) -> None:
                if name == "after_post_install_verification":
                    layout["receipt"].write_text("racing operator\n", encoding="utf-8")

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_writer_handles", return_value=[]),
                self.assertRaisesRegex(installer.CandidateInstallError, "rolled back"),
            ):
                installer.install_candidate(
                    **arguments, fault_injector=create_racing_receipt
                )
            self.assertEqual(_sha256(layout["formal"]), source_sha)
            self.assertEqual(_sha256(layout["candidate"]), candidate_sha)
            self.assertEqual(
                layout["receipt"].read_text(encoding="utf-8"),
                "racing operator\n",
            )

    def test_lsof_parser_allows_readers_and_reports_writers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "database.sqlite3"
            database.write_bytes(b"database")
            output = "\n".join(
                (
                    "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME",
                    f"python 101 mark 3rR REG 1,2 8 1 {database}",
                    f"python 202 mark 4uW REG 1,2 8 1 {database}",
                    f"python 303 mark 5wR REG 1,2 8 1 {database}",
                )
            )
            completed = subprocess.CompletedProcess(
                args=["lsof"], returncode=0, stdout=output, stderr=""
            )
            with patch.object(installer.subprocess, "run", return_value=completed):
                writers = installer._database_writer_handles((database,))
            self.assertEqual(
                [(item["pid"], item["descriptor"]) for item in writers],
                [(202, "4uW"), (303, "5wR")],
            )

            failed = subprocess.CompletedProcess(
                args=["lsof"], returncode=2, stdout="", stderr="permission denied"
            )
            with (
                patch.object(installer.subprocess, "run", return_value=failed),
                self.assertRaisesRegex(
                    installer.CandidateInstallError, "permission denied"
                ),
            ):
                installer._database_writer_handles((database,))

    def test_cli_requires_all_explicit_inputs_and_forwards_exact_values(self) -> None:
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            installer.build_parser().parse_args([])

        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            arguments = self._install_arguments(layout)
            argv = [
                "--formal-db",
                str(layout["formal"]),
                "--candidate",
                str(layout["candidate"]),
                "--expected-source-sha256",
                str(arguments["expected_source_sha256"]),
                "--expected-candidate-sha256",
                str(arguments["expected_candidate_sha256"]),
                "--backup-dir",
                str(layout["backup"]),
                "--receipt",
                str(layout["receipt"]),
                "--freeze-lock",
                str(layout["lock"]),
            ]
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                patch.object(
                    installer,
                    "install_candidate",
                    return_value={"status": "installed"},
                ) as invoked,
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                code = installer.main(argv)
            self.assertEqual((code, stderr.getvalue()), (0, ""))
            self.assertEqual(json.loads(stdout.getvalue()), {"status": "installed"})
            invoked.assert_called_once_with(**arguments)

    def test_exact_formal_target_is_not_configurable_by_cli(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            alternate = layout["formal"].with_name("alternate.sqlite3")
            self._build_source_database(alternate)
            arguments = self._install_arguments(layout)
            arguments["formal_database"] = alternate
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                self.assertRaisesRegex(
                    installer.CandidateInstallError, "formal target must be exactly"
                ),
            ):
                installer.install_candidate(**arguments)


if __name__ == "__main__":
    unittest.main()
