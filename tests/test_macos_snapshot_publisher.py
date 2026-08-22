from __future__ import annotations

import importlib.util
import os
import plistlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import ModuleType
from unittest.mock import patch
from zoneinfo import ZoneInfo

from v8.contracts import CURRENT_REPORT_RULE_VERSION, CURRENT_REPORT_VERSION
from v8.release_management_v9 import TARGET_RELEASE_ID, TAXONOMY_VERSION
from v8.storage import CURRENT_SCHEMA_MIGRATION_NAME, SCHEMA_VERSION


ROOT = Path(__file__).resolve().parents[1]
MACOS_DEPLOY = ROOT / "deploy/macos"
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _load_module(name: str, path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


publisher = _load_module(
    "dcar_macos_snapshot_publisher", MACOS_DEPLOY / "publish_snapshot.py"
)


def _runtime_identity(*, matcher_sha256: str = "a" * 64) -> dict[str, object]:
    return {
        "schema": "dcar-runtime-identity-v1",
        "report_version": CURRENT_REPORT_VERSION,
        "database_schema_version": SCHEMA_VERSION,
        "database_schema_migration": CURRENT_SCHEMA_MIGRATION_NAME,
        "active_release_id": "evaluation-v9__selling-points-v5.2",
        "active_release_status": "active",
        "rule_version": "evaluation-v9",
        "taxonomy_version": "selling-points-v5.2",
        "taxonomy_status": "published",
        "matcher_rule_sha256": matcher_sha256,
    }


def _writer_database(
    path: Path,
    *,
    latest_published_at: str,
    cutoff_status: str = "succeeded",
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE scheduler_runs(
                id INTEGER PRIMARY KEY,
                job_id TEXT,
                scheduled_for TEXT,
                status TEXT,
                completed_at TEXT
            );
            CREATE TABLE scheduler_run_attempts(
                id INTEGER PRIMARY KEY,
                scheduler_run_id INTEGER,
                attempt_number INTEGER,
                status TEXT,
                completed_at TEXT
            );
            CREATE TABLE content_items(id INTEGER, published_at TEXT);
            CREATE TABLE schema_migrations(version INTEGER, name TEXT);
            CREATE TABLE taxonomy_versions(version TEXT, status TEXT);
            CREATE TABLE evaluation_releases(
                id TEXT,
                rule_version TEXT,
                taxonomy_version TEXT,
                matcher_rule_sha256 TEXT,
                status TEXT
            );
            INSERT INTO scheduler_runs VALUES(
                1,'daily_capture','2026-08-10T18:00:00Z','partial',
                '2026-08-10T18:30:00Z'
            );
            INSERT INTO scheduler_runs VALUES(
                2,'daily_report','2026-08-11T00:00:00Z','partial',
                '2026-08-11T00:01:00Z'
            );
            INSERT INTO scheduler_runs VALUES(
                3,'weekly_report','2026-08-10T00:30:00Z','succeeded',
                '2026-08-10T00:31:00Z'
            );
            INSERT INTO scheduler_run_attempts VALUES(
                1,2,1,'partial','2026-08-11T00:01:00Z'
            );
            INSERT INTO scheduler_run_attempts VALUES(
                2,3,1,'succeeded','2026-08-10T00:31:00Z'
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
        connection.execute("PRAGMA user_version=16")
        connection.execute(
            "INSERT INTO scheduler_runs VALUES(?,?,?,?,?)",
            (
                4,
                "daily_media_cutoff",
                "2026-08-10T23:30:00Z",
                cutoff_status,
                "2026-08-10T23:45:00Z",
            ),
        )
        connection.execute(
            "INSERT INTO content_items VALUES(1,?)", (latest_published_at,)
        )
        connection.commit()
    finally:
        connection.close()
    completed_timestamp = datetime.fromisoformat(
        "2026-08-10T23:45:00+00:00"
    ).timestamp()
    os.utime(path, (completed_timestamp, completed_timestamp))


def _fetch_writer(url: str) -> dict[str, object]:
    if url.endswith("/health"):
        return {
            "status": "ok",
            "database": "dcar_insight.sqlite3",
            "database_state": {"runtime_identity": _runtime_identity()},
        }
    if url.endswith("/scheduler"):
        return {
            "requested": True,
            "enabled": True,
            "writer_lock": {"held": True},
            "startup_catchup": {
                "mode": "report_only",
                "requested": True,
                "enabled": True,
                "status": "succeeded",
                "error": None,
                "results": [
                    {
                        "job_id": "daily_report",
                        "scheduled_for": "2026-08-11T00:00:00Z",
                        "status": "partial",
                    },
                    {
                        "job_id": "weekly_report",
                        "scheduled_for": "2026-08-10T00:30:00Z",
                        "status": "succeeded",
                    },
                ],
            },
        }
    raise AssertionError(url)


class FakeRunner:
    def __init__(
        self,
        *,
        free_bytes: int = 100_000_000_000,
        dry_run_bytes: int = 1_024,
        fail_install: bool = False,
    ):
        self.free_bytes = free_bytes
        self.dry_run_bytes = dry_run_bytes
        self.fail_install = fail_install
        self.commands: list[list[str]] = []

    def __call__(
        self, arguments: list[str], **_: object
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append(list(arguments))
        rendered = " ".join(arguments)
        if " -G " in f" {rendered} ":
            identity = str(Path.home() / ".ssh/id_ed25519_dcar_test")
            return subprocess.CompletedProcess(
                arguments,
                0,
                stdout=f"hostname example.invalid\nuser deploy\nidentityfile {identity}\n",
                stderr="",
            )
        if "statvfs" in rendered:
            return subprocess.CompletedProcess(
                arguments, 0, stdout=f"{self.free_bytes}\n", stderr=""
            )
        if "--dry-run" in arguments:
            return subprocess.CompletedProcess(
                arguments,
                0,
                stdout=(f"Total transferred file size: {self.dry_run_bytes:,} bytes\n"),
                stderr="",
            )
        if self.fail_install and " install --bundle " in f" {rendered} ":
            return subprocess.CompletedProcess(
                arguments, 1, stdout="", stderr="remote install refused"
            )
        return subprocess.CompletedProcess(arguments, 0, stdout="{}\n", stderr="")


class MacOSSnapshotPublisherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.project = self.root / "project"
        self.project.mkdir()
        self.database = self.project / "dcar_insight.sqlite3"
        _writer_database(self.database, latest_published_at="2026-08-10T12:00:00Z")
        self.fake_home = self.root / "home"
        (self.fake_home / ".ssh").mkdir(parents=True)
        (self.fake_home / ".ssh/known_hosts").write_text(
            "example.invalid ssh-ed25519 public-host-key\n", encoding="utf-8"
        )
        identity = self.fake_home / ".ssh/id_ed25519_dcar_test"
        identity.write_text("not-a-real-key", encoding="utf-8")
        identity.chmod(0o600)
        self.env_file = self.root / "publisher.env"
        self.snapshot_root = self.root / "snapshots"
        self.env_file.write_text(
            "\n".join(
                [
                    "DCAR_PUBLISH_SSH_ALIAS=dcar-prod",
                    "DCAR_PUBLISH_REMOTE_PROJECT_ROOT=/var/www/dcar-aigc/current",
                    "DCAR_PUBLISH_REMOTE_STATE_ROOT=/var/lib/dcar-aigc",
                    "DCAR_PUBLISH_REMOTE_PYTHON=/var/www/dcar-aigc/current/.venv/bin/python",
                    f"DCAR_PUBLISH_SNAPSHOT_ROOT={self.snapshot_root}",
                    "DCAR_PUBLISH_MIN_REMOTE_FREE_BYTES=5368709120",
                    "DCAR_PUBLISH_EXPECTED_USER_VERSION=16",
                    "DCAR_PUBLISH_MAX_CONTENT_LAG_DAYS=1",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        self.env_file.chmod(0o600)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def config(self) -> object:
        return publisher._read_external_env(self.env_file, project_root=self.project)

    def fake_builder(self, **arguments: object) -> dict[str, object]:
        output = Path(arguments["output"])
        (output / "databases").mkdir(parents=True)
        (output / "databases/dcar_insight.sqlite3").write_bytes(b"snapshot-db")
        (output / "cache-files-from0").write_bytes(b"")
        (output / "reports-files-from0").write_bytes(b"")
        return {
            "snapshot_id": "20260811T010000Z-aaaaaaaaaaaa",
            "runtime_identity": _runtime_identity(),
            "databases": [
                {
                    "name": "dcar_insight.sqlite3",
                    "byte_size": 11,
                    "sha256": "a" * 64,
                }
            ],
            "artifact_policy": publisher.ARTIFACT_POLICY,
            "file_byte_size": 71_303_168,
            "optional_reuse_byte_size": 40_667_885_776,
        }

    def test_launch_agent_is_disabled_and_runs_only_at_0900(self) -> None:
        template = (
            MACOS_DEPLOY / "cn.tj.dcar.snapshot-publisher.plist.template"
        ).read_text(encoding="utf-8")
        rendered = template.replace("__PROJECT_ROOT_XML__", "/tmp/DcarAIGC")
        rendered = rendered.replace("__HOME_XML__", "/tmp/dcar-home")
        value = plistlib.loads(rendered.encode("utf-8"))
        environment = value["EnvironmentVariables"]
        self.assertEqual(value["Label"], "cn.tj.dcar.snapshot-publisher")
        self.assertTrue(value["Disabled"])
        self.assertEqual(value["StartCalendarInterval"], {"Hour": 9, "Minute": 0})
        self.assertNotIn("RunAtLoad", value)
        self.assertNotIn("KeepAlive", value)
        self.assertEqual(environment["DCAR_SCHEDULER_ENABLED"], "0")
        self.assertEqual(environment["DCAR_STARTUP_CATCHUP_ENABLED"], "0")
        self.assertFalse(any("API_KEY" in key for key in environment))

    def test_renderer_check_has_no_install_or_launchctl_side_effect(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(MACOS_DEPLOY / "render_snapshot_publisher.py"),
                "--project-root",
                str(ROOT),
                "--home",
                str(self.fake_home),
                "--check",
            ],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self.assertIn("valid disabled LaunchAgent", result.stdout)
        self.assertFalse((self.fake_home / "Library/LaunchAgents").exists())

    def test_external_environment_rejects_provider_credentials(self) -> None:
        with self.env_file.open("a", encoding="utf-8") as stream:
            stream.write("TIKHUB_API_KEY=forbidden\n")
        with self.assertRaises(publisher.SnapshotPublishError):
            self.config()

    def test_external_environment_pins_current_schema(self) -> None:
        self.assertEqual(self.config().expected_user_version, SCHEMA_VERSION)

    def test_publisher_identity_constants_match_current_runtime_contracts(self) -> None:
        self.assertEqual(publisher.EXPECTED_REPORT_VERSION, CURRENT_REPORT_VERSION)
        self.assertEqual(publisher.EXPECTED_DATABASE_SCHEMA_VERSION, SCHEMA_VERSION)
        self.assertEqual(
            publisher.EXPECTED_DATABASE_SCHEMA_MIGRATION,
            CURRENT_SCHEMA_MIGRATION_NAME,
        )
        self.assertEqual(publisher.EXPECTED_ACTIVE_RELEASE_ID, TARGET_RELEASE_ID)
        self.assertEqual(
            publisher.EXPECTED_RULE_VERSION, CURRENT_REPORT_RULE_VERSION
        )
        self.assertEqual(publisher.EXPECTED_TAXONOMY_VERSION, TAXONOMY_VERSION)

    def test_external_environment_rejects_symlink(self) -> None:
        symlink = self.root / "publisher-link.env"
        symlink.symlink_to(self.env_file)
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher._read_external_env(symlink, project_root=self.project)

    def test_external_environment_rejects_relative_snapshot_root(self) -> None:
        value = self.env_file.read_text(encoding="utf-8").replace(
            f"DCAR_PUBLISH_SNAPSHOT_ROOT={self.snapshot_root}",
            "DCAR_PUBLISH_SNAPSHOT_ROOT=relative-snapshots",
        )
        self.env_file.write_text(value, encoding="utf-8")
        self.env_file.chmod(0o600)
        with self.assertRaises(publisher.SnapshotPublishError):
            self.config()

    def test_wrapper_and_example_keep_scheduler_off_and_credentials_external(
        self,
    ) -> None:
        wrapper = (MACOS_DEPLOY / "run_snapshot_publisher.sh").read_text(
            encoding="utf-8"
        )
        example = (MACOS_DEPLOY / "publisher.env.example").read_text(encoding="utf-8")
        self.assertTrue(os.access(MACOS_DEPLOY / "run_snapshot_publisher.sh", os.X_OK))
        self.assertIn('"${DCAR_SCHEDULER_ENABLED:-}" == "0"', wrapper)
        self.assertIn('"${DCAR_STARTUP_CATCHUP_ENABLED:-}" == "0"', wrapper)
        self.assertIn("/usr/bin/caffeinate -s", wrapper)
        self.assertIn("TIKHUB_API_KEY_FILE", wrapper)
        self.assertNotIn("TIKHUB_API_KEY=", example)
        self.assertNotIn("PASSWORD=", example)
        self.assertNotIn("PRIVATE_KEY=", example)
        self.assertIn("40,739,188,944", example)
        self.assertIn("DCAR_PUBLISH_EXPECTED_USER_VERSION=16", example)

    def test_writer_freshness_accepts_today_partial_capture_but_rejects_stale_content(
        self,
    ) -> None:
        current = datetime(2026, 8, 11, 9, 0, tzinfo=SHANGHAI)
        value = publisher.check_writer_freshness(
            self.database,
            now=current,
            maximum_content_lag_days=1,
            fetch_json=_fetch_writer,
        )
        self.assertEqual(value.capture_status, "partial")
        self.assertEqual(value.media_cutoff_status, "succeeded")
        self.assertEqual(value.latest_published_at, "2026-08-10T12:00:00Z")

        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE scheduler_runs SET status='failed' WHERE job_id='daily_capture'"
            )
            connection.commit()
        completed_timestamp = datetime.fromisoformat(
            "2026-08-10T23:45:00+00:00"
        ).timestamp()
        os.utime(self.database, (completed_timestamp, completed_timestamp))
        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "status is not publishable: failed"
        ):
            publisher.check_writer_freshness(
                self.database,
                now=current,
                maximum_content_lag_days=1,
                fetch_json=_fetch_writer,
            )
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE scheduler_runs SET status='partial' WHERE job_id='daily_capture'"
            )
            connection.commit()

        stale = self.root / "stale.sqlite3"
        _writer_database(stale, latest_published_at="2026-08-08T12:00:00Z")
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher.check_writer_freshness(
                stale,
                now=current,
                maximum_content_lag_days=1,
                fetch_json=lambda url: {
                    **_fetch_writer(url),
                    **(
                        {"database": "stale.sqlite3"} if url.endswith("/health") else {}
                    ),
                },
            )

    def test_writer_freshness_requires_successful_cutoff_and_held_writer_lock(
        self,
    ) -> None:
        current = datetime(2026, 8, 11, 9, 0, tzinfo=SHANGHAI)
        failed_cutoff = self.root / "failed-cutoff.sqlite3"
        _writer_database(
            failed_cutoff,
            latest_published_at="2026-08-10T12:00:00Z",
            cutoff_status="failed",
        )
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher.check_writer_freshness(
                failed_cutoff,
                now=current,
                maximum_content_lag_days=1,
                fetch_json=lambda url: {
                    **_fetch_writer(url),
                    **(
                        {"database": "failed-cutoff.sqlite3"}
                        if url.endswith("/health")
                        else {}
                    ),
                },
            )

        def unlocked_writer(url: str) -> dict[str, object]:
            value = _fetch_writer(url)
            if url.endswith("/scheduler"):
                value = {**value, "writer_lock": {"held": False}}
            return value

        with self.assertRaises(publisher.SnapshotPublishError):
            publisher.check_writer_freshness(
                self.database,
                now=current,
                maximum_content_lag_days=1,
                fetch_json=unlocked_writer,
            )

    def test_writer_freshness_rejects_health_or_database_identity_drift(self) -> None:
        current = datetime(2026, 8, 11, 9, 0, tzinfo=SHANGHAI)

        def drifted_health(url: str) -> dict[str, object]:
            value = _fetch_writer(url)
            if url.endswith("/health"):
                value = {
                    **value,
                    "database_state": {
                        "runtime_identity": _runtime_identity(
                            matcher_sha256="b" * 64
                        )
                    },
                }
            return value

        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "does not match the formal database"
        ):
            publisher.check_writer_freshness(
                self.database,
                now=current,
                maximum_content_lag_days=1,
                fetch_json=drifted_health,
            )

        wrong_release = self.root / "wrong-release.sqlite3"
        _writer_database(
            wrong_release, latest_published_at="2026-08-10T12:00:00Z"
        )
        with sqlite3.connect(wrong_release) as connection:
            connection.execute(
                """
                UPDATE evaluation_releases
                SET id='evaluation-v8__selling-points-v5.2',rule_version='evaluation-v8'
                """
            )
            connection.commit()

        def wrong_release_health(url: str) -> dict[str, object]:
            value = _fetch_writer(url)
            if url.endswith("/health"):
                identity = _runtime_identity()
                identity["active_release_id"] = (
                    "evaluation-v8__selling-points-v5.2"
                )
                identity["rule_version"] = "evaluation-v8"
                value = {
                    **value,
                    "database": wrong_release.name,
                    "database_state": {"runtime_identity": identity},
                }
            return value

        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "active_release_id"
        ):
            publisher.check_writer_freshness(
                wrong_release,
                now=current,
                maximum_content_lag_days=1,
                fetch_json=wrong_release_health,
            )

    def test_writer_freshness_rejects_incomplete_or_non_report_startup_catchup(
        self,
    ) -> None:
        current = datetime(2026, 8, 11, 9, 0, tzinfo=SHANGHAI)

        def catchup_status(status: str) -> object:
            def fetch(url: str) -> dict[str, object]:
                value = _fetch_writer(url)
                if url.endswith("/scheduler"):
                    catchup = dict(value["startup_catchup"])
                    catchup["status"] = status
                    value = {**value, "startup_catchup": catchup}
                return value

            return fetch

        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "has not succeeded"
        ):
            publisher.check_writer_freshness(
                self.database,
                now=current,
                maximum_content_lag_days=1,
                fetch_json=catchup_status("running"),
            )

        def malicious_capture(url: str) -> dict[str, object]:
            value = _fetch_writer(url)
            if url.endswith("/scheduler"):
                catchup = dict(value["startup_catchup"])
                catchup["results"] = [
                    {
                        "job_id": "daily_capture",
                        "scheduled_for": "2026-08-10T18:00:00Z",
                        "status": "succeeded",
                    }
                ]
                value = {**value, "startup_catchup": catchup}
            return value

        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "contains a non-report job"
        ):
            publisher.check_writer_freshness(
                self.database,
                now=current,
                maximum_content_lag_days=1,
                fetch_json=malicious_capture,
            )

        runner = FakeRunner()
        with (
            patch.object(publisher.Path, "home", return_value=self.fake_home),
            self.assertRaisesRegex(
                publisher.SnapshotPublishError, "contains a non-report job"
            ),
        ):
            publisher.publish_snapshot(
                project_root=self.project,
                database=self.database,
                legacy_database=None,
                config=self.config(),
                now=current,
                runner=runner,
                fetch_json=malicious_capture,
                build_snapshot=lambda **_arguments: self.fail(
                    "snapshot build must not run after malicious catch-up"
                ),
            )
        self.assertEqual(runner.commands, [])

        for bad_result in (
            {
                "job_id": "daily_report",
                "scheduled_for": "2026-08-11T00:00:00Z",
                "status": "deferred",
            },
            {
                "job_id": "daily_report",
                "scheduled_for": "2026-08-11T00:00:00Z",
                "status": "failed",
            },
        ):
            def non_terminal_report(
                url: str, *, result: dict[str, str] = bad_result
            ) -> dict[str, object]:
                value = _fetch_writer(url)
                if url.endswith("/scheduler"):
                    catchup = dict(value["startup_catchup"])
                    catchup["results"] = [result]
                    value = {**value, "startup_catchup": catchup}
                return value

            with self.assertRaisesRegex(
                publisher.SnapshotPublishError, "non-terminal report result"
            ):
                publisher.check_writer_freshness(
                    self.database,
                    now=current,
                    maximum_content_lag_days=1,
                    fetch_json=non_terminal_report,
                )

        def mismatched_report(url: str) -> dict[str, object]:
            value = _fetch_writer(url)
            if url.endswith("/scheduler"):
                catchup = dict(value["startup_catchup"])
                catchup["results"] = [
                    {
                        "job_id": "daily_report",
                        "scheduled_for": "2026-08-11T00:00:00Z",
                        "status": "succeeded",
                    }
                ]
                value = {**value, "startup_catchup": catchup}
            return value

        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "does not match the database"
        ):
            publisher.check_writer_freshness(
                self.database,
                now=current,
                maximum_content_lag_days=1,
                fetch_json=mismatched_report,
            )

    def test_database_symlinks_are_rejected_before_network_or_snapshot(self) -> None:
        database_link = self.root / "database-link.sqlite3"
        database_link.symlink_to(self.database)
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher.check_writer_freshness(
                database_link,
                maximum_content_lag_days=1,
                fetch_json=lambda _url: self.fail("network fetch must not run"),
            )

        legacy_target = self.root / "legacy.sqlite3"
        legacy_target.write_bytes(b"not-used")
        legacy_link = self.root / "legacy-link.sqlite3"
        legacy_link.symlink_to(legacy_target)
        runner = FakeRunner()
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher.publish_snapshot(
                project_root=self.project,
                database=self.database,
                legacy_database=legacy_link,
                config=self.config(),
                runner=runner,
                fetch_json=lambda _url: self.fail("network fetch must not run"),
                build_snapshot=self.fake_builder,
            )
        self.assertEqual(runner.commands, [])

    def test_rsync_stats_parser_accepts_macos_openrsync_byte_unit(self) -> None:
        def runner(
            arguments: list[str], **_: object
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                arguments,
                0,
                stdout="Total transferred file size: 1234 B\n",
                stderr="",
            )

        self.assertEqual(
            publisher._rsync_transfer_bytes(
                runner,
                ["rsync", "source/", "destination/"],
                timeout=1,
            ),
            1_234,
        )

    def test_publish_uses_strict_ssh_dry_run_space_gate_verify_then_install(
        self,
    ) -> None:
        runner = FakeRunner()
        current = datetime(2026, 8, 11, 9, 0, tzinfo=SHANGHAI)
        with patch.object(publisher.Path, "home", return_value=self.fake_home):
            receipt = publisher.publish_snapshot(
                project_root=self.project,
                database=self.database,
                legacy_database=None,
                config=self.config(),
                now=current,
                runner=runner,
                fetch_json=_fetch_writer,
                build_snapshot=self.fake_builder,
            )
        self.assertEqual(receipt["snapshot_id"], "20260811T010000Z-aaaaaaaaaaaa")
        self.assertEqual(receipt["media_cutoff_status"], "succeeded")
        self.assertEqual(receipt["media_cutoff_scheduled_for"], "2026-08-10T23:30:00Z")
        self.assertGreaterEqual(receipt["required_remote_bytes"], 71_303_168)
        self.assertEqual(receipt["artifact_manifest_bytes"], 71_303_168)
        self.assertEqual(
            receipt["optional_reuse_manifest_bytes"], 40_667_885_776
        )
        self.assertEqual(receipt["rsync_dry_run_transfer_bytes"], 3_072)
        rendered = [" ".join(command) for command in runner.commands]
        self.assertTrue(any("BatchMode=yes" in command for command in rendered))
        self.assertTrue(
            any("StrictHostKeyChecking=yes" in command for command in rendered)
        )
        self.assertTrue(any("IdentitiesOnly=yes" in command for command in rendered))
        self.assertEqual(sum("--dry-run" in command for command in rendered), 3)
        staging = "/var/lib/dcar-aigc/incoming/20260811T010000Z-aaaaaaaaaaaa"
        self.assertTrue(
            any(f"{staging}/artifacts/cache/" in command for command in rendered)
        )
        self.assertTrue(
            any(f"{staging}/artifacts/reports/" in command for command in rendered)
        )
        self.assertTrue(any(f"{staging}/bundle/" in command for command in rendered))
        self.assertTrue(
            any(
                "--link-dest=/var/lib/dcar-aigc/cache" in command
                for command in rendered
            )
        )
        self.assertTrue(
            any(
                "--link-dest=/var/lib/dcar-aigc/reports" in command
                for command in rendered
            )
        )
        self.assertFalse(
            any(
                "dcar-prod:/var/lib/dcar-aigc/cache/" in command for command in rendered
            )
        )
        self.assertFalse(
            any(
                "dcar-prod:/var/lib/dcar-aigc/reports/" in command
                for command in rendered
            )
        )
        verify_index = next(
            index
            for index, command in enumerate(rendered)
            if " verify --bundle " in f" {command} "
        )
        install_index = next(
            index
            for index, command in enumerate(rendered)
            if " install --bundle " in f" {command} "
        )
        self.assertLess(verify_index, install_index)
        self.assertIn("sudo -n", rendered[verify_index])
        self.assertIn(f"--bundle {staging}/bundle", rendered[verify_index])
        receipt_path = (
            self.snapshot_root / "snapshot-20260811T010000Z/publisher-receipt.json"
        )
        self.assertTrue(receipt_path.is_file())

    def test_publish_refuses_snapshot_identity_drift_before_remote_staging(self) -> None:
        runner = FakeRunner()
        current = datetime(2026, 8, 11, 9, 0, tzinfo=SHANGHAI)

        def drifted_builder(**arguments: object) -> dict[str, object]:
            manifest = self.fake_builder(**arguments)
            manifest["runtime_identity"] = _runtime_identity(
                matcher_sha256="b" * 64
            )
            return manifest

        with (
            patch.object(publisher.Path, "home", return_value=self.fake_home),
            self.assertRaisesRegex(
                publisher.SnapshotPublishError, "drifted from the verified writer"
            ),
        ):
            publisher.publish_snapshot(
                project_root=self.project,
                database=self.database,
                legacy_database=None,
                config=self.config(),
                now=current,
                runner=runner,
                fetch_json=_fetch_writer,
                build_snapshot=drifted_builder,
            )
        rendered = [" ".join(command) for command in runner.commands]
        self.assertFalse(any("install -d" in command for command in rendered))
        self.assertFalse(any(command.startswith("rsync ") for command in rendered))

    def test_manifest_space_gate_stops_before_remote_staging_or_rsync(self) -> None:
        runner = FakeRunner(free_bytes=5_400_000_000)
        current = datetime(2026, 8, 11, 9, 0, tzinfo=SHANGHAI)
        with patch.object(publisher.Path, "home", return_value=self.fake_home):
            with self.assertRaises(publisher.SnapshotPublishError):
                publisher.publish_snapshot(
                    project_root=self.project,
                    database=self.database,
                    legacy_database=None,
                    config=self.config(),
                    now=current,
                    runner=runner,
                    fetch_json=_fetch_writer,
                    build_snapshot=self.fake_builder,
                )
        rendered = [" ".join(command) for command in runner.commands]
        self.assertFalse(any("install -d" in command for command in rendered))
        self.assertFalse(any(command.startswith("rsync ") for command in rendered))

    def test_rsync_transfer_space_gate_stops_before_real_transfer_or_install(
        self,
    ) -> None:
        runner = FakeRunner(
            free_bytes=100_000_000_000,
            dry_run_bytes=40_000_000_000,
        )
        current = datetime(2026, 8, 11, 9, 0, tzinfo=SHANGHAI)
        with patch.object(publisher.Path, "home", return_value=self.fake_home):
            with self.assertRaises(publisher.SnapshotPublishError):
                publisher.publish_snapshot(
                    project_root=self.project,
                    database=self.database,
                    legacy_database=None,
                    config=self.config(),
                    now=current,
                    runner=runner,
                    fetch_json=_fetch_writer,
                    build_snapshot=self.fake_builder,
                )
        rendered = [" ".join(command) for command in runner.commands]
        rsync_commands = [
            command for command in rendered if command.startswith("rsync ")
        ]
        self.assertEqual(len(rsync_commands), 3)
        self.assertTrue(all("--dry-run" in command for command in rsync_commands))
        self.assertFalse(
            any(" verify --bundle " in f" {command} " for command in rendered)
        )
        self.assertFalse(
            any(" install --bundle " in f" {command} " for command in rendered)
        )

    def test_remote_install_failure_retains_local_snapshot_and_receives_no_retry_delete(
        self,
    ) -> None:
        runner = FakeRunner(fail_install=True)
        current = datetime(2026, 8, 11, 9, 0, tzinfo=SHANGHAI)
        database_before = self.database.read_bytes()
        with patch.object(publisher.Path, "home", return_value=self.fake_home):
            with self.assertRaises(publisher.SnapshotPublishError):
                publisher.publish_snapshot(
                    project_root=self.project,
                    database=self.database,
                    legacy_database=None,
                    config=self.config(),
                    now=current,
                    runner=runner,
                    fetch_json=_fetch_writer,
                    build_snapshot=self.fake_builder,
                )
        snapshot = self.snapshot_root / "snapshot-20260811T010000Z"
        self.assertTrue(snapshot.is_dir())
        self.assertEqual(self.database.read_bytes(), database_before)
        rendered = "\n".join(" ".join(command) for command in runner.commands)
        self.assertNotIn(" rm ", f" {rendered} ")
        self.assertNotIn("--delete", rendered)


if __name__ == "__main__":
    unittest.main()
