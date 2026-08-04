from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from v8 import api
from v8.storage import connect, initialize_database


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "prepare_v9_freeze", ROOT / "scripts" / "prepare_v9_freeze.py"
)
assert SPEC is not None and SPEC.loader is not None
freeze = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(freeze)


class PrepareV9FreezeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "source.sqlite3"
        self.lock = self.root / "operator-freeze.lock"
        self.lock.touch()
        artifact = self.root / "artifact.txt"
        artifact.write_text("verified evidence", encoding="utf-8")
        artifact_directory = self.root / "comment-pages"
        artifact_directory.mkdir()
        comment_page = artifact_directory / "page_001.json"
        comment_page.write_text('{"comments": []}', encoding="utf-8")
        connection = sqlite3.connect(self.db)
        connection.executescript(
            """
            CREATE TABLE content_items(
                id INTEGER PRIMARY KEY, title TEXT, body TEXT,
                raw_account_uid TEXT, raw_account_name TEXT,
                content_type TEXT,
                manual_content_direction TEXT
            );
            CREATE TABLE provider_raw_responses(
                id INTEGER PRIMARY KEY, content_id INTEGER, operation TEXT,
                sha256 TEXT, captured_at TEXT
            );
            CREATE TABLE evidence_artifacts(
                id INTEGER PRIMARY KEY, content_id INTEGER, artifact_type TEXT,
                local_path TEXT, status TEXT, byte_size INTEGER, sha256 TEXT,
                created_at TEXT
            );
            CREATE TABLE comment_evidence_versions(
                id INTEGER PRIMARY KEY, content_id INTEGER, sha256 TEXT,
                captured_at TEXT
            );
            CREATE TABLE manual_evidence(
                id INTEGER PRIMARY KEY, content_id INTEGER, evidence_type TEXT,
                text_value TEXT, local_path TEXT, sha256 TEXT
            );
            CREATE TABLE evidence_envelopes(
                id INTEGER PRIMARY KEY, content_id INTEGER, evidence_sha256 TEXT
            );
            INSERT INTO content_items
            VALUES (1, 'title', 'body', 'uid', 'name', 'video', NULL);
            """
        )
        payload = artifact.read_bytes()
        import hashlib

        connection.execute(
            """
            INSERT INTO evidence_artifacts(
                id,content_id,artifact_type,local_path,status,byte_size,sha256,created_at
            ) VALUES (1,1,'media',?,'available',?,?, '2026-08-04T00:00:00Z')
            """,
            (str(artifact), len(payload), hashlib.sha256(payload).hexdigest()),
        )
        comment_payload = comment_page.read_bytes()
        directory_digest = hashlib.sha256()
        directory_digest.update(b"page_001.json\0")
        directory_digest.update(
            hashlib.sha256(comment_payload).hexdigest().encode("ascii")
        )
        directory_digest.update(b"\0")
        connection.execute(
            """
            INSERT INTO evidence_artifacts(
                id,content_id,artifact_type,local_path,status,byte_size,sha256,created_at
            ) VALUES (2,1,'comments',?,'available',?,?, '2026-08-04T00:00:00Z')
            """,
            (
                str(artifact_directory),
                len(comment_payload),
                directory_digest.hexdigest(),
            ),
        )
        connection.commit()
        connection.close()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_bundle_is_read_only_and_allows_missing_envelope(self) -> None:
        before = self.db.read_bytes()
        bundle = freeze.create_freeze_bundle(
            database=self.db,
            output_root=self.root / "backups",
            freeze_lock=self.lock,
            protected_ports=(),
            require_no_writer_handles=False,
        )
        self.assertEqual(self.db.read_bytes(), before)
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["source_total_changes"], 0)
        self.assertEqual(
            manifest["content_evidence_inventory"]["envelope_states"],
            {"absent": 1},
        )
        self.assertEqual(
            manifest["evidence_artifact_inventory"]["integrity_states"],
            {"verified": 2},
        )
        backup = bundle / manifest["database_backup"]["path"]
        self.assertFalse(Path(str(backup) + "-wal").exists())
        self.assertFalse(Path(str(backup) + "-shm").exists())
        copied = sqlite3.connect(backup)
        try:
            self.assertEqual(
                copied.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 1
            )
        finally:
            copied.close()

    def test_missing_operator_lock_fails_before_output(self) -> None:
        self.lock.unlink()
        with self.assertRaisesRegex(freeze.FreezeError, "freeze lock is missing"):
            freeze.create_freeze_bundle(
                database=self.db,
                output_root=self.root / "backups",
                freeze_lock=self.lock,
                protected_ports=(),
                require_no_writer_handles=False,
            )


class ProductionFreezeLifespanTest(unittest.IsolatedAsyncioTestCase):
    async def test_default_database_startup_is_blocked_before_initialization(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            lock = Path(temp) / "operator-freeze.lock"
            lock.touch()
            config = api.ApiConfig(
                db_path=api.DEFAULT_DB,
                reports_root=root / "reports",
                legacy_db_path=root / "legacy.sqlite3",
                operator_freeze_lock=lock,
            )
            application = api.create_app(config)
            with patch.object(api, "initialize_database") as initialize:
                with self.assertRaisesRegex(RuntimeError, "operator freeze lock"):
                    async with api.lifespan(application):
                        pass
                initialize.assert_not_called()


class ApiConfigTest(unittest.TestCase):
    def test_environment_defaults_disable_scheduler_and_catchup(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            config = api.ApiConfig.from_env()
        self.assertFalse(config.scheduler_enabled)
        self.assertFalse(config.startup_catchup_enabled)
        self.assertFalse(config.effective_startup_catchup_enabled)

    def test_environment_paths_and_two_key_catchup_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            values = {
                "DCAR_V8_DB": str(root / "current.sqlite3"),
                "DCAR_V8_REPORTS_ROOT": str(root / "reports"),
                "DCAR_LEGACY_DB": str(root / "legacy.sqlite3"),
                "DCAR_OPERATOR_FREEZE_LOCK": str(root / "freeze.lock"),
                "DCAR_SCHEDULER_ENABLED": "1",
                "DCAR_STARTUP_CATCHUP_ENABLED": "1",
            }
            with patch.dict(os.environ, values, clear=True):
                config = api.ApiConfig.from_env()
        self.assertEqual(config.db_path, root / "current.sqlite3")
        self.assertEqual(config.reports_root, root / "reports")
        self.assertEqual(config.legacy_db_path, root / "legacy.sqlite3")
        self.assertEqual(config.operator_freeze_lock, root / "freeze.lock")
        self.assertTrue(config.effective_startup_catchup_enabled)

    def test_catchup_cannot_bypass_disabled_scheduler(self) -> None:
        config = api.ApiConfig(
            db_path=Path("current.sqlite3"),
            reports_root=Path("reports"),
            legacy_db_path=Path("legacy.sqlite3"),
            operator_freeze_lock=Path("freeze.lock"),
            scheduler_enabled=False,
            startup_catchup_enabled=True,
        )
        self.assertFalse(config.effective_startup_catchup_enabled)


class ApiLifespanSwitchTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _seed_report_runtime(database: Path) -> None:
        with connect(database) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('taxonomy-v51','selling-points-v5.1','published','test',
                          '2026-08-04T00:00:00Z','2026-08-04T00:00:00Z')
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
                    "2026-08-04T00:00:00Z",
                    "2026-08-04T00:00:00Z",
                    "2026-08-04T00:00:00Z",
                ),
            )
            connection.commit()

    async def test_scheduler_and_catchup_follow_two_key_policy(self) -> None:
        cases = (
            (False, False, False, False),
            (False, True, False, False),
            (True, False, True, False),
            (True, True, True, True),
        )
        for (
            scheduler_enabled,
            catchup_enabled,
            expect_scheduler,
            expect_thread,
        ) in cases:
            with (
                self.subTest(
                    scheduler=scheduler_enabled,
                    catchup=catchup_enabled,
                ),
                tempfile.TemporaryDirectory() as temp,
            ):
                root = Path(temp)
                config = api.ApiConfig(
                    db_path=root / "current.sqlite3",
                    reports_root=root / "reports",
                    legacy_db_path=root / "legacy.sqlite3",
                    operator_freeze_lock=root / "freeze.lock",
                    scheduler_enabled=scheduler_enabled,
                    startup_catchup_enabled=catchup_enabled,
                )
                self._seed_report_runtime(config.db_path)
                application = api.create_app(config)
                scheduler = MagicMock()
                catchup_thread = MagicMock()
                with (
                    patch.object(api, "BackgroundScheduler", return_value=scheduler),
                    patch.object(api, "install_jobs") as install_jobs,
                    patch.object(api.threading, "Thread", return_value=catchup_thread),
                    patch.object(
                        api,
                        "recover_stale_fetch_slots",
                        return_value={"stale_candidates": 0, "recovered": 0},
                    ),
                    patch.object(
                        api,
                        "recover_stale_media_processing_slots",
                        return_value={"stale_candidates": 0, "recovered": 0},
                    ),
                    patch.object(api, "_recover_interrupted_tasks", return_value=0),
                ):
                    async with api.lifespan(application):
                        self.assertEqual(
                            application.state.scheduler_enabled,
                            scheduler_enabled,
                        )
                        self.assertEqual(
                            application.state.scheduler_requested,
                            scheduler_enabled,
                        )
                        self.assertEqual(
                            application.state.startup_catchup_enabled,
                            scheduler_enabled and catchup_enabled,
                        )
                    self.assertEqual(install_jobs.called, expect_scheduler)
                    self.assertEqual(scheduler.start.called, expect_scheduler)
                    self.assertEqual(catchup_thread.start.called, expect_thread)

    async def test_unready_runtime_blocks_scheduler_and_catchup_but_not_api(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = api.ApiConfig(
                db_path=root / "current.sqlite3",
                reports_root=root / "reports",
                legacy_db_path=root / "legacy.sqlite3",
                operator_freeze_lock=root / "freeze.lock",
                scheduler_enabled=True,
                startup_catchup_enabled=True,
            )
            application = api.create_app(config)
            scheduler = MagicMock()
            catchup_thread = MagicMock()
            with (
                patch.object(api, "BackgroundScheduler", return_value=scheduler),
                patch.object(api, "install_jobs") as install_jobs,
                patch.object(api.threading, "Thread", return_value=catchup_thread),
                patch.object(
                    api,
                    "recover_stale_fetch_slots",
                    return_value={"stale_candidates": 0, "recovered": 0},
                ),
                patch.object(
                    api,
                    "recover_stale_media_processing_slots",
                    return_value={"stale_candidates": 0, "recovered": 0},
                ),
                patch.object(api, "_recover_interrupted_tasks", return_value=0),
            ):
                async with api.lifespan(application):
                    self.assertTrue(application.state.scheduler_requested)
                    self.assertFalse(application.state.scheduler_enabled)
                    self.assertFalse(application.state.startup_catchup_enabled)
                    self.assertFalse(application.state.report_runtime_ready)
                    self.assertIn(
                        "active evaluation release",
                        application.state.report_runtime_error,
                    )
                    self.assertEqual(application.state.catchup_status, "blocked")
                install_jobs.assert_not_called()
                scheduler.start.assert_not_called()
                catchup_thread.start.assert_not_called()

    async def test_unsafe_legacy_automatic_report_blocks_scheduler_start(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "current.sqlite3"
            self._seed_report_runtime(database)
            with connect(database) as connection:
                connection.execute(
                    """
                    INSERT INTO taxonomy_versions(
                        id,version,status,definition,created_at,published_at
                    ) VALUES ('taxonomy-v5','selling-points-v5.0','retired','legacy',
                              '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                    """
                )
                connection.execute(
                    """
                    INSERT INTO evaluation_releases(
                        id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                        created_at,updated_at,retired_at
                    ) VALUES ('evaluation-v7__selling-points-v5.0','evaluation-v7',
                              'selling-points-v5.0',?,'retired',?,?,?)
                    """,
                    (
                        "7" * 64,
                        "2026-08-01T00:00:00Z",
                        "2026-08-01T00:00:00Z",
                        "2026-08-04T09:00:00Z",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO report_tasks(
                        id,task_type,name,period_start,period_end,creation_source,
                        task_status,created_at,updated_at
                    ) VALUES ('unsafe-report','daily','unsafe','2026-08-01',
                              '2026-08-01','automatic','partial',?,?)
                    """,
                    ("2026-08-04T07:45:50Z", "2026-08-04T07:45:50Z"),
                )
                connection.execute(
                    """
                    INSERT INTO report_revisions(
                        task_id,revision,release_id,contract_version,rule_version,
                        taxonomy_version,report_json_path,report_sha256,created_at
                    ) VALUES ('unsafe-report',1,
                              'evaluation-v7__selling-points-v5.0',
                              'dcar-content-operations-report-v8.3','evaluation-v7',
                              'selling-points-v5.0','reports/unsafe.json',?,?)
                    """,
                    ("7" * 64, "2026-08-04T07:45:50Z"),
                )
                connection.commit()
            config = api.ApiConfig(
                db_path=database,
                reports_root=root / "reports",
                legacy_db_path=root / "legacy.sqlite3",
                operator_freeze_lock=root / "freeze.lock",
                scheduler_enabled=True,
                startup_catchup_enabled=True,
            )
            application = api.create_app(config)
            scheduler = MagicMock()
            catchup_thread = MagicMock()
            with (
                patch.object(api, "BackgroundScheduler", return_value=scheduler),
                patch.object(api, "install_jobs") as install_jobs,
                patch.object(api.threading, "Thread", return_value=catchup_thread),
                patch.object(
                    api,
                    "recover_stale_fetch_slots",
                    return_value={"stale_candidates": 0, "recovered": 0},
                ),
                patch.object(
                    api,
                    "recover_stale_media_processing_slots",
                    return_value={"stale_candidates": 0, "recovered": 0},
                ),
                patch.object(api, "_recover_interrupted_tasks", return_value=0),
            ):
                async with api.lifespan(application):
                    self.assertFalse(application.state.scheduler_enabled)
                    self.assertIn(
                        "outside the active release",
                        application.state.report_runtime_error,
                    )
                install_jobs.assert_not_called()
                scheduler.start.assert_not_called()
                catchup_thread.start.assert_not_called()


if __name__ == "__main__":
    unittest.main()
