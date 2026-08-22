from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import sqlite3
import stat
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

from v8.contracts import CURRENT_REPORT_RULE_VERSION, CURRENT_REPORT_VERSION
from v8.release_management_v9 import (
    TARGET_RELEASE_ID,
    TAXONOMY_VERSION,
)
from v8.storage import CURRENT_SCHEMA_MIGRATION_NAME, SCHEMA_VERSION


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


builder = _load_module(
    "dcar_build_server_snapshot",
    REPOSITORY_ROOT / "scripts" / "build_server_snapshot.py",
)
installer = _load_module(
    "dcar_install_server_snapshot",
    REPOSITORY_ROOT / "deploy" / "server" / "install_snapshot.py",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_bundle_manifest(bundle: Path, value: dict[str, object]) -> None:
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    (bundle / "manifest.json").write_bytes(payload)
    (bundle / "manifest.sha256").write_text(
        hashlib.sha256(payload).hexdigest() + "  manifest.json\n",
        encoding="ascii",
    )


def _create_main_database(path: Path, project_root: Path) -> None:
    cache_root = project_root / "data/cache"
    cache_root.mkdir(parents=True)
    for name, payload in (
        (".comment_hash_salt", b"c" * 32),
        (".platform_user_salt", b"p" * 32),
    ):
        salt = cache_root / name
        salt.write_bytes(payload)
        salt.chmod(0o600)
    report = project_root / "reports/runs/v8/test/revision_001/report.json"
    report.parent.mkdir(parents=True)
    report.write_text('{"status":"ok"}\n', encoding="utf-8")
    report.chmod(0o600)
    video = project_root / "data/cache/v8/media/test/video.mp4"
    video.parent.mkdir(parents=True)
    video.write_bytes(b"test-video-evidence")
    media_manifest = project_root / "data/cache/v8/media/test/media.json"
    media_manifest.write_text(
        json.dumps({"video_path": str(video)}) + "\n",
        encoding="utf-8",
    )
    media_manifest.chmod(0o600)

    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            PRAGMA user_version=16;
            CREATE TABLE content_items(
                id INTEGER PRIMARY KEY,
                published_at TEXT,
                imported_at TEXT
            );
            CREATE TABLE scheduler_runs(
                job_id TEXT,
                scheduled_for TEXT,
                status TEXT,
                completed_at TEXT
            );
            CREATE TABLE schema_migrations(version INTEGER, name TEXT);
            CREATE TABLE taxonomy_versions(version TEXT, status TEXT);
            CREATE TABLE evaluation_releases(
                id TEXT,
                rule_version TEXT,
                taxonomy_version TEXT,
                matcher_rule_sha256 TEXT,
                status TEXT
            );
            CREATE TABLE evidence_artifacts(
                local_path TEXT,
                sha256 TEXT,
                byte_size INTEGER,
                status TEXT,
                artifact_type TEXT
            );
            CREATE TABLE report_files(
                local_path TEXT,
                sha256 TEXT,
                byte_size INTEGER,
                status TEXT
            );
            CREATE TABLE report_revisions(
                report_json_path TEXT,
                report_sha256 TEXT
            );
            INSERT INTO content_items VALUES(
                1,'2026-08-10T01:00:00Z','2026-08-10T02:00:00Z'
            );
            INSERT INTO scheduler_runs VALUES(
                'daily_capture','2026-08-10T18:00:00Z','succeeded',
                '2026-08-10T18:10:00Z'
            );
            INSERT INTO schema_migrations VALUES(
                16,'remove-manual-review'
            );
            INSERT INTO taxonomy_versions VALUES(
                'selling-points-v5.2','published'
            );
            INSERT INTO evaluation_releases VALUES(
                'evaluation-v9__selling-points-v5.2','evaluation-v9',
                'selling-points-v5.2',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                'active'
            );
            """
        )
        connection.execute(
            "INSERT INTO evidence_artifacts VALUES(?,?,?,?,?)",
            (
                "data/cache/v8/media/test/media.json",
                _sha256(media_manifest),
                media_manifest.stat().st_size,
                "available",
                "media_manifest",
            ),
        )
        connection.execute(
            "INSERT INTO report_files VALUES(?,?,?,?)",
            (
                "reports/runs/v8/test/revision_001/report.json",
                _sha256(report),
                report.stat().st_size,
                "available",
            ),
        )
        connection.execute(
            "INSERT INTO report_revisions VALUES(?,?)",
            ("reports/runs/v8/test/revision_001/report.json", _sha256(report)),
        )
        connection.commit()
    finally:
        connection.close()


def _create_legacy_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            "PRAGMA user_version=3; CREATE TABLE legacy_marker(value TEXT);"
            "INSERT INTO legacy_marker VALUES('legacy');"
        )
        connection.commit()
    finally:
        connection.close()


def _create_old_active_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            "PRAGMA user_version=16; CREATE TABLE active_marker(value TEXT);"
            "INSERT INTO active_marker VALUES('old');"
        )
        connection.commit()
    finally:
        connection.close()


class ServerSnapshotDeploymentTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.project = self.root / "project"
        self.project.mkdir()
        self.database = self.project / "app/data/dcar_insight.sqlite3"
        self.database.parent.mkdir(parents=True)
        self.legacy_database = self.project / "app/data/web_mvp.sqlite3"
        _create_main_database(self.database, self.project)
        _create_legacy_database(self.legacy_database)
        self.bundle = self.root / "bundle"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def build_bundle(self) -> dict[str, object]:
        return builder.build_snapshot(
            project_root=self.project,
            database=self.database,
            legacy_database=self.legacy_database,
            output=self.bundle,
            expected_user_version=SCHEMA_VERSION,
        )

    def server_config(self) -> object:
        return installer.InstallConfig(
            database_root=self.root / "server/db",
            cache_root=self.root / "server/cache",
            reports_root=self.root / "server/reports",
            runtime_root=self.root / "server/runtime",
        )

    def stage_artifacts(self, manifest: dict[str, object], config: object) -> None:
        staged_root = self.bundle.parent / "artifacts"
        for item in manifest["files"]:
            source = self.project / item["project_path"]
            root = staged_root / item["root"]
            target = root / item["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)

    def test_builder_creates_online_backups_and_hash_lists(self) -> None:
        manifest = self.build_bundle()
        self.assertEqual(manifest["schema"], builder.BUNDLE_SCHEMA)
        self.assertEqual(
            manifest["runtime_identity"],
            {
                "schema": "dcar-runtime-identity-v1",
                "report_version": CURRENT_REPORT_VERSION,
                "database_schema_version": SCHEMA_VERSION,
                "database_schema_migration": CURRENT_SCHEMA_MIGRATION_NAME,
                "active_release_id": "evaluation-v9__selling-points-v5.2",
                "active_release_status": "active",
                "rule_version": "evaluation-v9",
                "taxonomy_version": "selling-points-v5.2",
                "taxonomy_status": "published",
                "matcher_rule_sha256": "a" * 64,
            },
        )
        self.assertEqual(manifest["artifact_policy"]["name"], "thin-server-v1")
        self.assertEqual(manifest["file_count"], 4)
        self.assertEqual(
            {item["project_path"] for item in manifest["files"]},
            {
                "data/cache/.comment_hash_salt",
                "data/cache/.platform_user_salt",
                "data/cache/v8/media/test/media.json",
                "reports/runs/v8/test/revision_001/report.json",
            },
        )
        self.assertEqual(manifest["optional_reuse_file_count"], 1)
        self.assertEqual(
            manifest["optional_reuse_files"][0]["project_path"],
            "data/cache/v8/media/test/video.mp4",
        )
        self.assertTrue((self.bundle / "manifest.sha256").is_file())
        self.assertTrue(
            (self.bundle / "cache-files-from0").read_bytes().endswith(b"\0")
        )
        self.assertTrue(
            (self.bundle / "reports-files-from0").read_bytes().endswith(b"\0")
        )
        snapshot = sqlite3.connect(self.bundle / "databases/dcar_insight.sqlite3")
        try:
            self.assertEqual(
                snapshot.execute("PRAGMA user_version").fetchone()[0],
                SCHEMA_VERSION,
            )
            self.assertEqual(
                snapshot.execute("SELECT COUNT(*) FROM content_items").fetchone()[0],
                1,
            )
        finally:
            snapshot.close()

    def test_server_runbook_pins_current_schema(self) -> None:
        runbook = (REPOSITORY_ROOT / "deploy/server/README.md").read_text(
            encoding="utf-8"
        )
        self.assertIn(f"--expected-user-version {SCHEMA_VERSION}", runbook)

    def test_deployment_identity_constants_match_current_runtime_contracts(
        self,
    ) -> None:
        for module in (builder, installer):
            self.assertEqual(module.EXPECTED_REPORT_VERSION, CURRENT_REPORT_VERSION)
            self.assertEqual(
                module.EXPECTED_DATABASE_SCHEMA_VERSION, SCHEMA_VERSION
            )
            self.assertEqual(
                module.EXPECTED_DATABASE_SCHEMA_MIGRATION,
                CURRENT_SCHEMA_MIGRATION_NAME,
            )
            self.assertEqual(module.EXPECTED_ACTIVE_RELEASE_ID, TARGET_RELEASE_ID)
            self.assertEqual(module.EXPECTED_RULE_VERSION, CURRENT_REPORT_RULE_VERSION)
            self.assertEqual(module.EXPECTED_TAXONOMY_VERSION, TAXONOMY_VERSION)

    def test_builder_refuses_wrong_release_or_schema_migration_identity(self) -> None:
        for statement in (
            """
            UPDATE evaluation_releases
            SET id='evaluation-v8__selling-points-v5.2',rule_version='evaluation-v8'
            """,
            """
            UPDATE schema_migrations
            SET name='interaction-user-v1-fallback-keys' WHERE version=16
            """,
        ):
            candidate = self.root / f"candidate-{len(list(self.root.glob('candidate-*')))}.sqlite3"
            shutil.copy2(self.database, candidate)
            with sqlite3.connect(candidate) as connection:
                connection.execute(statement)
                connection.commit()
            output = candidate.with_suffix(".bundle")
            with self.assertRaisesRegex(
                builder.SnapshotBuildError, "runtime identity mismatch"
            ):
                builder.build_snapshot(
                    project_root=self.project,
                    database=candidate,
                    output=output,
                    expected_user_version=SCHEMA_VERSION,
                )
            self.assertFalse(output.exists())

    def test_installer_rejects_self_consistent_wrong_release_before_service_stop(
        self,
    ) -> None:
        self.build_bundle()
        config = self.server_config()
        database_path = self.bundle / "databases/dcar_insight.sqlite3"
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                """
                UPDATE evaluation_releases
                SET id='evaluation-v8__selling-points-v5.2',rule_version='evaluation-v8'
                """
            )
            connection.commit()
        manifest_path = self.bundle / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime_identity"]["active_release_id"] = (
            "evaluation-v8__selling-points-v5.2"
        )
        manifest["runtime_identity"]["rule_version"] = "evaluation-v8"
        main = next(
            item
            for item in manifest["databases"]
            if item["name"] == "dcar_insight.sqlite3"
        )
        main["byte_size"] = database_path.stat().st_size
        main["sha256"] = _sha256(database_path)
        _write_bundle_manifest(self.bundle, manifest)
        actions: list[str] = []
        with self.assertRaisesRegex(
            installer.SnapshotInstallError, "active_release_id"
        ):
            installer.install_bundle(
                self.bundle,
                config,
                service_action=actions.append,
                smoke_check=lambda: None,
            )
        self.assertEqual(actions, [])

    def test_installer_rejects_legacy_bundle_without_runtime_identity(self) -> None:
        self.build_bundle()
        config = self.server_config()
        manifest = json.loads(
            (self.bundle / "manifest.json").read_text(encoding="utf-8")
        )
        manifest.pop("runtime_identity")
        _write_bundle_manifest(self.bundle, manifest)
        actions: list[str] = []
        with self.assertRaisesRegex(
            installer.SnapshotInstallError, "runtime identity has an invalid shape"
        ):
            installer.install_bundle(
                self.bundle,
                config,
                service_action=actions.append,
                smoke_check=lambda: None,
            )
        self.assertEqual(actions, [])

    def test_installer_rejects_self_consistent_wrong_schema_before_service_stop(
        self,
    ) -> None:
        self.build_bundle()
        config = self.server_config()
        database_path = self.bundle / "databases/dcar_insight.sqlite3"
        with sqlite3.connect(database_path) as connection:
            connection.execute("DELETE FROM schema_migrations")
            connection.execute(
                "INSERT INTO schema_migrations VALUES(12,'append-only-metric-observations')"
            )
            connection.execute("PRAGMA user_version=12")
            connection.commit()
        manifest = json.loads(
            (self.bundle / "manifest.json").read_text(encoding="utf-8")
        )
        manifest["runtime_identity"]["database_schema_version"] = 12
        manifest["runtime_identity"]["database_schema_migration"] = (
            "append-only-metric-observations"
        )
        main = next(
            item
            for item in manifest["databases"]
            if item["name"] == "dcar_insight.sqlite3"
        )
        main["user_version"] = 12
        main["byte_size"] = database_path.stat().st_size
        main["sha256"] = _sha256(database_path)
        _write_bundle_manifest(self.bundle, manifest)
        actions: list[str] = []
        with self.assertRaisesRegex(
            installer.SnapshotInstallError, "database_schema_version"
        ):
            installer.install_bundle(
                self.bundle,
                config,
                service_action=actions.append,
                smoke_check=lambda: None,
            )
        self.assertEqual(actions, [])

    def test_installer_rejects_database_identity_drift_from_signed_manifest(
        self,
    ) -> None:
        self.build_bundle()
        config = self.server_config()
        database_path = self.bundle / "databases/dcar_insight.sqlite3"
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                "UPDATE evaluation_releases SET matcher_rule_sha256=?",
                ("b" * 64,),
            )
            connection.commit()
        manifest = json.loads(
            (self.bundle / "manifest.json").read_text(encoding="utf-8")
        )
        main = next(
            item
            for item in manifest["databases"]
            if item["name"] == "dcar_insight.sqlite3"
        )
        main["byte_size"] = database_path.stat().st_size
        main["sha256"] = _sha256(database_path)
        _write_bundle_manifest(self.bundle, manifest)
        actions: list[str] = []
        with self.assertRaisesRegex(
            installer.SnapshotInstallError, "does not match the manifest"
        ):
            installer.install_bundle(
                self.bundle,
                config,
                service_action=actions.append,
                smoke_check=lambda: None,
            )
        self.assertEqual(actions, [])

    def test_compose_keeps_replica_database_and_artifacts_read_only(self) -> None:
        compose = (REPOSITORY_ROOT / "deploy/server/compose.yml").read_text(
            encoding="utf-8"
        )
        api_section = compose.split("\n  web:\n", 1)[0]
        database_mount = api_section.split("source: /var/lib/dcar-aigc/db", 1)[1].split(
            "source: /var/lib/dcar-aigc/reports", 1
        )[0]
        reports_mount = api_section.split("source: /var/lib/dcar-aigc/reports", 1)[
            1
        ].split("source: /var/lib/dcar-aigc/cache", 1)[0]
        cache_mount = api_section.split("source: /var/lib/dcar-aigc/cache", 1)[1].split(
            "source: /var/lib/dcar-aigc/runtime", 1
        )[0]
        self.assertIn("    read_only: true", api_section)
        self.assertIn("read_only: true", database_mount)
        self.assertIn("read_only: true", reports_mount)
        self.assertIn("read_only: true", cache_mount)

    def test_systemd_replica_has_no_writable_data_path(self) -> None:
        service = (
            REPOSITORY_ROOT / "deploy/server/systemd/dcar-api.service"
        ).read_text(encoding="utf-8")
        self.assertNotIn("ReadWritePaths=", service)
        self.assertNotIn("BindPaths=", service)
        self.assertIn(
            "ExecStart=/var/www/dcar-aigc/current/.venv/bin/python", service
        )
        self.assertNotIn("/var/www/dcar-aigc/runtime/venv/bin/python", service)
        self.assertIn(
            "BindReadOnlyPaths=/var/lib/dcar-aigc/db:"
            "/var/www/dcar-aigc/current/app/data",
            service,
        )
        self.assertIn(
            "BindReadOnlyPaths=/var/lib/dcar-aigc/reports:"
            "/var/www/dcar-aigc/current/reports",
            service,
        )
        self.assertIn(
            "BindReadOnlyPaths=/var/lib/dcar-aigc/cache:"
            "/var/www/dcar-aigc/current/data/cache",
            service,
        )
        self.assertIn("ReadOnlyPaths=/var/lib/dcar-aigc/db", service)

    def test_builder_refuses_artifact_hash_drift_without_publishing_output(
        self,
    ) -> None:
        report = self.project / "reports/runs/v8/test/revision_001/report.json"
        report.write_text('{"status":"changed"}\n', encoding="utf-8")
        with self.assertRaises(builder.SnapshotBuildError):
            self.build_bundle()
        self.assertFalse(self.bundle.exists())

    def test_builder_can_manifest_registered_optional_media_without_local_bytes(
        self,
    ) -> None:
        video = self.project / "data/cache/v8/media/test/video.mp4"
        expected_sha = _sha256(video)
        expected_size = video.stat().st_size
        with sqlite3.connect(self.database) as connection:
            connection.execute("DELETE FROM evidence_artifacts")
            connection.execute(
                "INSERT INTO evidence_artifacts VALUES(?,?,?,?,?)",
                (
                    "data/cache/v8/media/test/video.mp4",
                    expected_sha,
                    expected_size,
                    "available",
                    "media",
                ),
            )
            connection.commit()
        video.unlink()
        manifest = self.build_bundle()
        self.assertEqual(manifest["optional_reuse_file_count"], 1)
        self.assertEqual(manifest["optional_reuse_files"][0]["sha256"], expected_sha)
        self.assertNotIn(b"video.mp4", (self.bundle / "cache-files-from0").read_bytes())

    def test_install_and_manual_rollback_replace_only_active_databases(self) -> None:
        manifest = self.build_bundle()
        config = self.server_config()
        config.database_root.mkdir(parents=True)
        _create_old_active_database(config.database_root / "dcar_insight.sqlite3")
        self.stage_artifacts(manifest, config)
        actions: list[str] = []

        receipt = installer.install_bundle(
            self.bundle,
            config,
            service_action=actions.append,
            smoke_check=lambda: None,
        )
        self.assertEqual(actions, ["stop", "start"])
        self.assertEqual(receipt["snapshot_id"], manifest["snapshot_id"])
        self.assertEqual(receipt["optional_reuse"]["omitted_count"], 1)
        active = sqlite3.connect(config.database_root / "dcar_insight.sqlite3")
        try:
            self.assertEqual(
                active.execute("SELECT COUNT(*) FROM content_items").fetchone()[0],
                1,
            )
        finally:
            active.close()
        installed_artifact = config.cache_root / "v8/media/test/media.json"
        self.assertTrue(installed_artifact.is_file())
        self.assertEqual(stat.S_IMODE(installed_artifact.stat().st_mode), 0o640)
        self.assertEqual(installed_artifact.stat().st_uid, os.getuid())
        self.assertEqual(installed_artifact.stat().st_gid, os.getgid())
        self.assertEqual(
            stat.S_IMODE((config.database_root / "dcar_insight.sqlite3").stat().st_mode),
            0o640,
        )
        self.assertEqual(stat.S_IMODE(installed_artifact.parent.stat().st_mode), 0o750)

        rollback_actions: list[str] = []
        rollback = installer.rollback_snapshot(
            config,
            snapshot_id=manifest["snapshot_id"],
            service_action=rollback_actions.append,
            smoke_check=lambda: None,
        )
        self.assertEqual(rollback_actions, ["stop", "start"])
        self.assertEqual(rollback["restored_from_snapshot"], manifest["snapshot_id"])
        restored = sqlite3.connect(config.database_root / "dcar_insight.sqlite3")
        try:
            self.assertEqual(
                restored.execute("SELECT value FROM active_marker").fetchone()[0],
                "old",
            )
        finally:
            restored.close()
        self.assertFalse(installed_artifact.exists())
        self.assertFalse((config.database_root / "web_mvp.sqlite3").exists())

    def test_installer_refuses_missing_artifact_before_stopping_service(self) -> None:
        manifest = self.build_bundle()
        config = self.server_config()
        config.database_root.mkdir(parents=True)
        _create_old_active_database(config.database_root / "dcar_insight.sqlite3")
        self.stage_artifacts(manifest, config)
        first = manifest["files"][0]
        target_root = self.bundle.parent / "artifacts" / first["root"]
        (target_root / first["path"]).unlink()
        actions: list[str] = []
        with self.assertRaises(installer.SnapshotInstallError):
            installer.install_bundle(
                self.bundle,
                config,
                service_action=actions.append,
                smoke_check=lambda: None,
            )
        self.assertEqual(actions, [])
        active = sqlite3.connect(config.database_root / "dcar_insight.sqlite3")
        try:
            self.assertEqual(
                active.execute("SELECT value FROM active_marker").fetchone()[0],
                "old",
            )
        finally:
            active.close()

    def test_installer_refuses_unknown_thin_policy_before_stopping_service(self) -> None:
        manifest = self.build_bundle()
        config = self.server_config()
        self.stage_artifacts(manifest, config)
        manifest_path = self.bundle / "manifest.json"
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
        value["artifact_policy"]["delete_unlisted"] = True
        payload = (
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        manifest_path.write_bytes(payload)
        (self.bundle / "manifest.sha256").write_text(
            hashlib.sha256(payload).hexdigest() + "  manifest.json\n",
            encoding="ascii",
        )
        actions: list[str] = []
        with self.assertRaises(installer.SnapshotInstallError):
            installer.install_bundle(
                self.bundle,
                config,
                service_action=actions.append,
                smoke_check=lambda: None,
            )
        self.assertEqual(actions, [])

    def test_optional_large_artifact_is_reused_only_when_hash_matches(self) -> None:
        manifest = self.build_bundle()
        config = self.server_config()
        config.database_root.mkdir(parents=True)
        _create_old_active_database(config.database_root / "dcar_insight.sqlite3")
        self.stage_artifacts(manifest, config)
        active_video = config.cache_root / "v8/media/test/video.mp4"
        active_video.parent.mkdir(parents=True)
        active_video.write_bytes(b"test-video-evidence")
        before = active_video.stat().st_ino
        receipt = installer.install_bundle(
            self.bundle,
            config,
            service_action=lambda _action: None,
            smoke_check=lambda: None,
        )
        self.assertEqual(receipt["optional_reuse"]["reused_count"], 1)
        self.assertEqual(receipt["optional_reuse"]["omitted_count"], 0)
        self.assertEqual(active_video.stat().st_ino, before)
        self.assertEqual(active_video.read_bytes(), b"test-video-evidence")

    def test_failed_smoke_check_automatically_restores_previous_database(self) -> None:
        manifest = self.build_bundle()
        config = self.server_config()
        config.database_root.mkdir(parents=True)
        _create_old_active_database(config.database_root / "dcar_insight.sqlite3")
        self.stage_artifacts(manifest, config)
        actions: list[str] = []
        smoke_calls = 0

        def smoke() -> None:
            nonlocal smoke_calls
            smoke_calls += 1
            if smoke_calls == 1:
                raise installer.SnapshotInstallError("new snapshot smoke failed")

        with self.assertRaises(installer.SnapshotInstallError):
            installer.install_bundle(
                self.bundle,
                config,
                service_action=actions.append,
                smoke_check=smoke,
            )
        self.assertEqual(actions, ["stop", "start", "stop", "start"])
        self.assertEqual(smoke_calls, 2)
        active = sqlite3.connect(config.database_root / "dcar_insight.sqlite3")
        try:
            self.assertEqual(
                active.execute("SELECT value FROM active_marker").fetchone()[0],
                "old",
            )
        finally:
            active.close()

    def test_runtime_identity_smoke_drift_automatically_restores_previous_snapshot(
        self,
    ) -> None:
        manifest = self.build_bundle()
        config = self.server_config()
        config.database_root.mkdir(parents=True)
        _create_old_active_database(config.database_root / "dcar_insight.sqlite3")
        self.stage_artifacts(manifest, config)
        actions: list[str] = []
        smoke_calls = 0

        def smoke() -> None:
            nonlocal smoke_calls
            smoke_calls += 1
            if smoke_calls == 1:
                raise installer.SnapshotInstallError(
                    "replica runtime identity is not the staged snapshot"
                )

        with self.assertRaisesRegex(
            installer.SnapshotInstallError, "previous databases were restored"
        ):
            installer.install_bundle(
                self.bundle,
                config,
                service_action=actions.append,
                smoke_check=smoke,
            )
        self.assertEqual(actions, ["stop", "start", "stop", "start"])
        self.assertEqual(smoke_calls, 2)
        with sqlite3.connect(
            config.database_root / "dcar_insight.sqlite3"
        ) as active:
            self.assertEqual(
                active.execute("SELECT value FROM active_marker").fetchone()[0],
                "old",
            )

    def test_failed_smoke_restores_overwritten_artifact_bytes(self) -> None:
        manifest = self.build_bundle()
        config = self.server_config()
        config.database_root.mkdir(parents=True)
        _create_old_active_database(config.database_root / "dcar_insight.sqlite3")
        self.stage_artifacts(manifest, config)
        active_artifact = config.cache_root / "v8/media/test/media.json"
        active_artifact.parent.mkdir(parents=True)
        active_artifact.write_bytes(b"old-ocr-compatible-artifact")
        smoke_calls = 0

        def smoke() -> None:
            nonlocal smoke_calls
            smoke_calls += 1
            if smoke_calls == 1:
                raise installer.SnapshotInstallError("new snapshot smoke failed")

        with self.assertRaises(installer.SnapshotInstallError):
            installer.install_bundle(
                self.bundle,
                config,
                service_action=lambda _action: None,
                smoke_check=smoke,
            )
        self.assertEqual(active_artifact.read_bytes(), b"old-ocr-compatible-artifact")

    def test_failed_service_stop_never_moves_the_active_database(self) -> None:
        manifest = self.build_bundle()
        config = self.server_config()
        config.database_root.mkdir(parents=True)
        _create_old_active_database(config.database_root / "dcar_insight.sqlite3")
        self.stage_artifacts(manifest, config)

        def fail_stop(action: str) -> None:
            self.assertEqual(action, "stop")
            raise installer.SnapshotInstallError("service did not stop")

        with self.assertRaises(installer.SnapshotInstallError):
            installer.install_bundle(
                self.bundle,
                config,
                service_action=fail_stop,
                smoke_check=lambda: None,
            )
        active = sqlite3.connect(config.database_root / "dcar_insight.sqlite3")
        try:
            self.assertEqual(
                active.execute("SELECT value FROM active_marker").fetchone()[0],
                "old",
            )
        finally:
            active.close()
        self.assertFalse((config.history_root / manifest["snapshot_id"]).exists())

    def test_default_smoke_check_matches_current_replica_api_shape(self) -> None:
        manifest = self.build_bundle()
        main_database = next(
            item
            for item in manifest["databases"]
            if item["name"] == "dcar_insight.sqlite3"
        )
        config = self.server_config()
        responses = {
            config.health_url: {
                "status": "ok",
                "read_only": True,
                "database_state": {
                    "sha256": main_database["sha256"],
                    "user_version": main_database["user_version"],
                    "content_count": manifest["freshness"]["content_count"],
                    "latest_published_at": manifest["freshness"]["latest_published_at"],
                    "runtime_identity": manifest["runtime_identity"],
                },
            },
            config.overview_url: {
                "status": "ready",
                "windows": {"yesterday": {}},
                "data_freshness": {
                    "status": "current",
                    "latest_published_at": manifest["freshness"]["latest_published_at"],
                },
            },
            config.scheduler_url: {
                "read_only": True,
                "requested": False,
                "enabled": False,
                "startup_catchup": {"requested": False},
            },
        }
        with patch.object(
            installer,
            "_read_json_url",
            side_effect=lambda url, _timeout: responses[url],
        ):
            installer._default_smoke_check(config, manifest)()

        drifted_health = dict(responses[config.health_url])
        drifted_database_state = dict(drifted_health["database_state"])
        drifted_identity = dict(drifted_database_state["runtime_identity"])
        drifted_identity["matcher_rule_sha256"] = "b" * 64
        drifted_database_state["runtime_identity"] = drifted_identity
        drifted_health["database_state"] = drifted_database_state
        drifted_responses = {**responses, config.health_url: drifted_health}
        short_config = replace(config, start_wait_seconds=0.01)
        with (
            patch.object(
                installer,
                "_read_json_url",
                side_effect=lambda url, _timeout: drifted_responses[url],
            ),
            patch.object(installer.time, "sleep", return_value=None),
            self.assertRaisesRegex(
                installer.SnapshotInstallError, "runtime identity"
            ),
        ):
            installer._default_smoke_check(short_config, manifest)()


if __name__ == "__main__":
    unittest.main()
