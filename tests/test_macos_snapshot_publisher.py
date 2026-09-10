from __future__ import annotations

import copy
import hashlib
import json
import os
import plistlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests.test_matrix_snapshot_publisher import DAY, ROOT, create_writer_fixture, load_publisher
from tests import test_server_snapshot_deployment as snapshot_fixture
from v8 import pipeline_cutover
from v8.contracts import CURRENT_REPORT_RULE_VERSION, CURRENT_REPORT_VERSION
from v8.release_management_v9 import TARGET_RELEASE_ID, TAXONOMY_VERSION
from v8.snapshot_contract import descriptor
from v8.storage import CURRENT_SCHEMA_MIGRATION_NAME, SCHEMA_VERSION

publisher = load_publisher()
MACOS_DEPLOY = ROOT / "deploy/macos"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class FakeRunner:
    """The entire deployment boundary is fake; local DB/evidence are real."""
    def __init__(self, identity, *, free_bytes=100_000_000_000, dry_run_bytes=1024,
                 fail_install=False, install_mismatch=False, fail_probe_after_install=False,
                 fail_verify=False, artifact_backup_bytes=0,
                 database_backup_bytes=4096, free_bytes_sequence=None):
        self.identity = identity
        self.free_bytes, self.dry_run_bytes = free_bytes, dry_run_bytes
        self.free_bytes_sequence = list(free_bytes_sequence or [])
        self.artifact_backup_bytes = artifact_backup_bytes
        self.database_backup_bytes = database_backup_bytes
        self.fail_install, self.install_mismatch = fail_install, install_mismatch
        self.fail_probe_after_install = fail_probe_after_install
        self.fail_verify = fail_verify
        self.active_snapshot_id = "20260828T010000Z-" + "b" * 12
        self.active_database_sha256 = "b" * 64
        self.install_attempted = False
        self.commands = []
        self.manifest = None
        self.manifest_sha256 = "b" * 64
        self.active_manifest_sha256 = "b" * 64
        self.transition = None

    def probe(self):
        return {
            "schema": publisher.REMOTE_PROBE_SCHEMA,
            "current_release": "/var/www/dcar-aigc/releases/test-release",
            "python_ready": True, "installer_ready": True,
            "services": {key: "active" for key in ("dcar-api.service", "dcar-web.service", "dcar-auth.service", "dcar-douyin-control.service")},
            "directories": {key: True for key in ("db", "cache", "reports", "runtime", "incoming")},
            "free_bytes": self.free_bytes, "schema_transition": self.transition,
            "active_receipt": {
                "schema": "dcar-read-replica-install-receipt-v1", "snapshot_id": self.active_snapshot_id,
                "database_sha256": {"dcar_insight.sqlite3": self.active_database_sha256},
                "runtime_identity": self.identity, "snapshot_contract": descriptor(),
                "artifact_policy": publisher.ARTIFACT_POLICY, "manifest_sha256": self.active_manifest_sha256,
            },
            "health": {"status": "ok", "read_only": True, "snapshot_contract": descriptor(),
                "database_state": {"sha256": self.active_database_sha256, "user_version": 19, "runtime_identity": self.identity}},
            "overview": {"status": "ready"},
            "scheduler": {"read_only": True, "requested": False, "enabled": False,
                          "startup_catchup": {"requested": False, "enabled": False}},
        }

    def __call__(self, arguments, **_kwargs):
        self.commands.append(list(arguments))
        rendered = " ".join(arguments)

        def result(value=None, *, code=0, error=""):
            return subprocess.CompletedProcess(arguments, code, stdout=json.dumps(value or {}) + "\n", stderr=error)

        if " -G " in f" {rendered} ":
            return subprocess.CompletedProcess(arguments, 0,
                stdout=f"hostname example.invalid\nuser deploy\nidentityfile {Path.home() / '.ssh/id_ed25519_dcar_test'}\n", stderr="")
        if publisher.REMOTE_PROBE_SCHEMA in rendered:
            if self.fail_probe_after_install and self.install_attempted:
                return result(code=255, error="temporary network failure")
            return result(self.probe())
        if (
            arguments[0] == "rsync"
            and arguments[-1].endswith("/bundle/")
            and "--dry-run" not in arguments
        ):
            path = Path(arguments[-2]) / "manifest.json"
            self.manifest = json.loads(path.read_bytes())
            self.manifest_sha256 = sha(path)
        if " install --bundle " in f" {rendered} ":
            self.install_attempted = True
            if self.fail_install:
                return result(code=1, error="remote install refused")
            assert self.manifest is not None
            self.active_snapshot_id = self.manifest["snapshot_id"]
            expected = self.manifest["databases"][0]["sha256"]
            self.active_database_sha256 = "c" * 64 if self.install_mismatch else expected
            self.active_manifest_sha256 = self.manifest_sha256
            return result({"schema": "dcar-read-replica-install-receipt-v1", "snapshot_id": self.active_snapshot_id,
                "database_sha256": {"dcar_insight.sqlite3": expected}, "runtime_identity": self.identity,
                "snapshot_contract": descriptor(), "artifact_policy": publisher.ARTIFACT_POLICY,
                "manifest_sha256": self.manifest_sha256})
        if " rollback " in f" {rendered} ":
            self.active_snapshot_id = "20260828T010000Z-" + "b" * 12
            self.active_database_sha256 = "b" * 64
            self.active_manifest_sha256 = "b" * 64
            return result({"schema": "dcar-read-replica-rollback-receipt-v1", "restored_from_snapshot": self.active_snapshot_id})
        if "dcar-install-headroom-v1" in rendered:
            return result({
                "schema": "dcar-install-headroom-v1",
                "artifact_backup_bytes": self.artifact_backup_bytes,
                "database_backup_bytes": self.database_backup_bytes,
            })
        if "statvfs" in rendered:
            free_bytes = (
                self.free_bytes_sequence.pop(0)
                if self.free_bytes_sequence
                else self.free_bytes
            )
            return subprocess.CompletedProcess(arguments, 0, stdout=f"{free_bytes}\n", stderr="")
        if "--dry-run" in rendered:
            paths = []
            root = None
            if "cache-files-from0" in rendered:
                root = "cache"
            elif "reports-files-from0" in rendered:
                root = "reports"
            elif "/artifacts/cache/" in rendered:
                root = "cache"
            elif "/artifacts/reports/" in rendered:
                root = "reports"
            if root is not None:
                if self.manifest is not None:
                    paths = [
                        item["path"]
                        for item in self.manifest["files"]
                        if item["root"] == root
                    ]
                else:
                    argument = next(
                        item for item in arguments
                        if item.startswith("--files-from=")
                    )
                    paths = [
                        item.decode()
                        for item in Path(argument.split("=", 1)[1]).read_bytes().split(b"\0")
                        if item
                    ]
            itemized = "".join(
                f">f+++++++++{publisher.RSYNC_ITEM_SEPARATOR}{path}\n"
                for path in paths
            )
            byte_size = self.dry_run_bytes
            if self.manifest is not None and root is None and "/bundle/" in rendered:
                byte_size = 0
            return subprocess.CompletedProcess(arguments, 0,
                stdout=itemized + f"Total transferred file size: {byte_size:,} bytes\n", stderr="")
        if " prune " in f" {rendered} ":
            return result({"schema": "dcar-read-replica-prune-receipt-v1", "active_snapshot_id": self.active_snapshot_id,
                           "retain_count": 2, "incoming": {"deleted": [], "reclaimed_bytes": 0},
                           "snapshot_history": {"deleted": [], "reclaimed_bytes": 0}, "reclaimed_bytes": 0})
        if " verify --bundle " in f" {rendered} ":
            if self.fail_verify:
                return result(code=1, error="remote verify refused")
            return result({"status": "verified", "snapshot_id": self.manifest["snapshot_id"],
                           "snapshot_contract": descriptor(), "manifest_sha256": self.manifest_sha256})
        return result()


class MacOSSnapshotPublisherTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="dcar-publisher-transport-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.fixture = create_writer_fixture(self.root / "project")
        self.project, self.database = self.fixture.project, self.fixture.database
        installed_database = self.root / "formal-data/dcar_insight.sqlite3"
        installed_database.parent.mkdir()
        self.database.replace(installed_database)
        self.fixture.database = installed_database
        self.database = installed_database
        self.now = self.fixture.now
        self.fake_home = self.root / "home"
        (self.fake_home / ".ssh").mkdir(parents=True)
        for name, body in (("known_hosts", "example.invalid ssh-ed25519 fixture-host-key\n"),
                           ("id_ed25519_dcar_test", "not-a-real-key")):
            path = self.fake_home / ".ssh" / name
            path.write_text(body)
            path.chmod(0o600)
        launch_agents = self.fake_home / "Library/LaunchAgents"
        launch_agents.mkdir(parents=True)
        writer_plist = launch_agents / "cn.tj.dcar.writer-worker.plist"
        writer_plist.write_bytes(
            plistlib.dumps(
                {
                    "Label": "cn.tj.dcar.writer-worker",
                    "WorkingDirectory": str(self.project),
                    "ProgramArguments": [
                        str(self.project / "deploy/macos/run_writer_worker.sh")
                    ],
                    "EnvironmentVariables": {
                        "DCAR_PROJECT_ROOT": str(self.project),
                        "DCAR_V8_DB": str(self.database),
                        "DCAR_WRITER_LOCK": str(self.fixture.writer_lock),
                        "DCAR_READ_ONLY": "0",
                        "DCAR_SCHEDULER_ENABLED": "1",
                        "DCAR_STARTUP_CATCHUP_ENABLED": "1",
                    },
                }
            )
        )
        writer_plist.chmod(0o600)
        os_home = patch.object(
            publisher.runtime_database.pwd,
            "getpwuid",
            return_value=SimpleNamespace(pw_dir=str(self.fake_home)),
        )
        os_home.start()
        self.addCleanup(os_home.stop)
        self.snapshot_root = self.root / "snapshots"
        self.env_file = self.root / "publisher.env"
        self.env_file.write_text("\n".join((
            "DCAR_PUBLISH_SSH_ALIAS=dcar-prod",
            "DCAR_PUBLISH_REMOTE_PROJECT_ROOT=/var/www/dcar-aigc/current",
            "DCAR_PUBLISH_REMOTE_STATE_ROOT=/var/lib/dcar-aigc",
            "DCAR_PUBLISH_REMOTE_PYTHON=/var/www/dcar-aigc/current/.venv/bin/python",
            f"DCAR_PUBLISH_SNAPSHOT_ROOT={self.snapshot_root}",
            "DCAR_PUBLISH_MIN_REMOTE_FREE_BYTES=5368709120",
            "DCAR_PUBLISH_EXPECTED_USER_VERSION=19",
            "DCAR_PUBLISH_MAX_CONTENT_LAG_DAYS=1", "")))
        self.env_file.chmod(0o600)
        self.last_manifest = None
        self.last_runner = None
        guard = patch("urllib.request.urlopen", side_effect=AssertionError("Real network forbidden"))
        guard.start()
        self.addCleanup(guard.stop)

    def config(self):
        return publisher._read_external_env(self.env_file, project_root=self.project)

    def runner(self, **kwargs):
        return FakeRunner(self.fixture.runtime_identity, **kwargs)

    def test_rsync_transfer_plan_uses_a_server_stable_separator(self):
        output = (
            f".d..tpog...{publisher.RSYNC_ITEM_SEPARATOR}v8/\n"
            f">f+++++++++{publisher.RSYNC_ITEM_SEPARATOR}v8/file|name.json\n"
            "Total transferred file size: 478 bytes\n"
        )

        self.assertEqual(publisher.RSYNC_ITEM_SEPARATOR, "|")
        self.assertEqual(
            publisher._parse_rsync_transfer_plan(output),
            publisher.RsyncTransferPlan(
                byte_size=478,
                changed_paths=("v8/file|name.json",),
            ),
        )

    def test_rsync_transfer_plan_accepts_macos_remote_sender_marker(self):
        output = (
            f"<f+++++++{publisher.RSYNC_ITEM_SEPARATOR}v8/file.json\n"
            "Total transferred file size: 478 B\n"
        )

        self.assertEqual(
            publisher._parse_rsync_transfer_plan(output),
            publisher.RsyncTransferPlan(
                byte_size=478,
                changed_paths=("v8/file.json",),
            ),
        )

    def test_changed_path_must_be_bound_to_snapshot_manifest(self):
        manifest = {
            "files": [
                {
                    "root": "cache",
                    "path": "v8/known.json",
                    "byte_size": 10,
                }
            ]
        }

        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "changed paths are not bound"
        ):
            publisher._manifest_artifact_bytes(
                manifest,
                root="cache",
                paths=("v8/foreign.json",),
            )

    def builder(self, **arguments):
        """Real detached DB plus complete fixture files, fake remote transport."""
        output = Path(arguments["output"])
        (output / "databases").mkdir(parents=True)
        database = output / "databases/dcar_insight.sqlite3"
        source = sqlite3.connect(f"{self.database.as_uri()}?mode=ro", uri=True)
        destination = sqlite3.connect(database)
        try:
            source.backup(destination)
        finally:
            source.close()
            destination.close()
        frozen = snapshot_fixture.builder._FrozenArtifacts(output / publisher.FROZEN_ARTIFACT_DIRECTORY)
        files = []
        for root_name, root in (("cache", self.project / "data/cache"), ("reports", self.project / "reports")):
            for path in sorted(root.rglob("*")):
                if path.is_file():
                    snapshot_fixture.builder._add_artifact(frozen, [], project_root=self.project,
                        relative_path=path.relative_to(self.project).as_posix())
                    files.append(frozen[(root_name, path.relative_to(root).as_posix())])
            (output / f"{root_name}-files-from0").write_bytes(b"".join(
                row["path"].encode() + b"\0" for row in files if row["root"] == root_name))
        database_sha = sha(database)
        manifest = {
            "schema": "dcar-read-replica-snapshot-v2", "snapshot_id": "20260829T010000Z-" + database_sha[:12],
            "runtime_identity": self.fixture.runtime_identity, "writer_project_root": str(self.project),
            "snapshot_contract": descriptor(), "artifact_policy": publisher.ARTIFACT_POLICY,
            "managed_originals": {"contract_version": "managed-originals-v1", "bundles": []},
            "databases": [{"name": "dcar_insight.sqlite3", "bundle_path": "databases/dcar_insight.sqlite3",
                "byte_size": database.stat().st_size, "sha256": database_sha, "user_version": SCHEMA_VERSION}],
            "files": files, "file_count": len(files), "file_byte_size": sum(row["byte_size"] for row in files),
            "local_artifact_source": {"contract": publisher.FROZEN_ARTIFACT_CONTRACT,
                                      "directory": publisher.FROZEN_ARTIFACT_DIRECTORY},
            "optional_reuse_files": [], "optional_reuse_byte_size": 0,
        }
        self.write_manifest(output, manifest)
        self.last_manifest = manifest
        return manifest

    def write_manifest(self, output, manifest):
        path = output / "manifest.json"
        path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
        (output / "manifest.sha256").write_text(sha(path) + "  manifest.json\n")

    def publish(self, *, runner=None, automatic=False, fetch=None, builder=None, now=None,
                resume_staged_snapshot_id=None):
        runner = runner or self.runner()
        self.last_runner = runner
        function = publisher.publish_snapshot_automatically if automatic else publisher.publish_snapshot
        with patch.object(publisher.Path, "home", return_value=self.fake_home), patch.object(
            publisher, "_utc_now", return_value=(now or self.now).astimezone(timezone.utc).isoformat()
        ):
            return function(project_root=self.project,
                database=(None if resume_staged_snapshot_id is not None else self.database),
                legacy_database=None,
                config=self.config(), now=now or self.now, runner=runner, fetch_json=fetch or self.fixture.fetch,
                build_snapshot=builder or self.builder,
                **({"resume_staged_snapshot_id": resume_staged_snapshot_id}
                   if resume_staged_snapshot_id is not None else {}))

    def staged_resume(self, *, output_name="snapshot-20260829T010000Z", **runner_kwargs):
        output = self.snapshot_root / output_name
        manifest = self.builder(
            project_root=self.project,
            database=self.database,
            legacy_database=None,
            output=output,
            expected_user_version=SCHEMA_VERSION,
        )
        observation = publisher._observe_formal_read_source(
            self.database,
            project_root=self.project,
            now=self.now,
            config=self.config(),
            fetch_json=self.fixture.fetch,
        )
        _identity, freshness, manifest_sha256, _bundle_bytes = (
            publisher._verify_local_snapshot(
                output,
                manifest,
                current=self.now,
                config=self.config(),
                project_root=self.project,
                fetch_json=lambda _url: self.fail(
                    "sealed snapshot verification queried writer health"
                ),
                sealed_freshness=observation.freshness,
                expected_runtime_identity=observation.freshness.runtime_identity,
            )
        )
        source_receipt = publisher._source_receipt_payload(
            manifest=manifest,
            manifest_sha256=manifest_sha256,
            freshness=freshness,
            database=observation.database,
            database_identity=observation.database_identity,
            writer_lock=observation.writer_lock,
        )
        publisher._write_json_exclusive(
            output / publisher.SOURCE_RECEIPT_FILENAME, source_receipt
        )
        runner = self.runner(**runner_kwargs)
        runner.manifest = manifest
        runner.manifest_sha256 = sha(output / "manifest.json")
        return output, manifest, runner

    def create_pending(self):
        runner = self.runner(fail_probe_after_install=True)
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "temporarily unavailable"):
            self.publish(runner=runner, automatic=True)
        runner.fail_probe_after_install = False
        return runner

    def freshness(self, fixture=None, *, fetch=None):
        fixture = fixture or self.fixture
        return publisher.check_writer_freshness(fixture.database, now=fixture.now,
            maximum_content_lag_days=1, fetch_json=fetch or fixture.fetch, project_root=fixture.project)

    def copy_database(self, name):
        target = self.root / name
        with sqlite3.connect(f"{self.database.as_uri()}?mode=ro", uri=True) as source:
            with sqlite3.connect(target) as destination:
                source.backup(destination)
        return target

    def runtime_database_identity(self, database):
        metadata = database.stat()
        return {
            "canonical_path": str(database.resolve()),
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "nlink": metadata.st_nlink,
            "access_mode": "writer",
        }

    def test_launch_agent_is_enabled_and_reconciles_hourly_from_0900(self):
        text = (MACOS_DEPLOY / "cn.tj.dcar.snapshot-publisher.plist.template").read_text()
        value = plistlib.loads(text.replace("__PROJECT_ROOT_XML__", "/tmp/DcarAIGC").replace("__HOME_XML__", "/tmp/dcar-home").encode())
        self.assertFalse(value["Disabled"])
        self.assertTrue(value["RunAtLoad"])
        self.assertEqual(value["StartCalendarInterval"], {"Hour": 9, "Minute": 0})
        self.assertEqual(value["StartInterval"], 3600)
        self.assertNotIn("KeepAlive", value)
        self.assertEqual(value["EnvironmentVariables"]["DCAR_SCHEDULER_ENABLED"], "0")
        self.assertEqual(value["EnvironmentVariables"]["DCAR_STARTUP_CATCHUP_ENABLED"], "0")
        self.assertEqual(
            value["EnvironmentVariables"]["DCAR_V8_DB"],
            "/tmp/dcar-home/Library/Application Support/DcarAIGC/data/"
            "dcar_insight.sqlite3",
        )
        self.assertEqual(
            value["EnvironmentVariables"]["DCAR_LEGACY_DB"],
            "/tmp/dcar-home/Library/Application Support/DcarAIGC/data/"
            "web_mvp.sqlite3",
        )
        self.assertFalse(any("API_KEY" in key for key in value["EnvironmentVariables"]))

    def test_renderer_check_has_no_install_or_launchctl_side_effect(self):
        before = {
            path.relative_to(self.fake_home)
            for path in self.fake_home.rglob("*")
        }
        result = subprocess.run([sys.executable, str(MACOS_DEPLOY / "render_snapshot_publisher.py"),
            "--project-root", str(ROOT), "--home", str(self.fake_home), "--check"],
            check=True, capture_output=True, text=True)
        self.assertIn("valid automatic LaunchAgent", result.stdout)
        self.assertEqual(
            before,
            {path.relative_to(self.fake_home) for path in self.fake_home.rglob("*")},
        )

    def test_external_environment_rejects_provider_credentials(self):
        self.env_file.write_text(self.env_file.read_text() + "TIKHUB_API_KEY=forbidden\n")
        with self.assertRaises(publisher.SnapshotPublishError):
            self.config()

    def test_external_environment_pins_current_schema(self):
        self.assertEqual(self.config().expected_user_version, SCHEMA_VERSION)
        for version in (16, 17, 18):
            self.env_file.write_text(self.env_file.read_text().replace("VERSION=19", f"VERSION={version}"))
            with self.assertRaisesRegex(publisher.SnapshotPublishError, "schema 19"):
                self.config()
            self.env_file.write_text(self.env_file.read_text().replace(f"VERSION={version}", "VERSION=19"))

    def test_writer_endpoint_timeout_allows_busy_writer_health_checks(self):
        response = unittest.mock.MagicMock()
        response.status = 200
        response.read.return_value = b'{"status":"ok"}'
        response.__enter__.return_value = response
        with patch("urllib.request.urlopen", return_value=response) as opener:
            self.assertEqual(
                publisher._default_fetch_json("http://127.0.0.1:8766/api/v8/health"),
                {"status": "ok"},
            )
        self.assertEqual(
            opener.call_args.kwargs["timeout"],
            publisher.WRITER_ENDPOINT_TIMEOUT_SECONDS,
        )
        self.assertEqual(publisher.WRITER_ENDPOINT_TIMEOUT_SECONDS, 120)

    def test_publisher_identity_constants_match_current_runtime_contracts(self):
        self.assertEqual(publisher.EXPECTED_REPORT_VERSION, CURRENT_REPORT_VERSION)
        self.assertEqual(publisher.EXPECTED_DATABASE_SCHEMA_VERSION, SCHEMA_VERSION)
        self.assertEqual(publisher.EXPECTED_DATABASE_SCHEMA_MIGRATION, CURRENT_SCHEMA_MIGRATION_NAME)
        self.assertEqual(publisher.EXPECTED_ACTIVE_RELEASE_ID, TARGET_RELEASE_ID)
        self.assertEqual(publisher.EXPECTED_RULE_VERSION, CURRENT_REPORT_RULE_VERSION)
        self.assertEqual(publisher.EXPECTED_TAXONOMY_VERSION, TAXONOMY_VERSION)
        self.assertEqual(
            publisher.FRESHNESS_SCHEMA,
            "profile-day-publication-freshness-v1",
        )
        self.assertEqual(
            publisher.TRANSITION_SCHEMA,
            "dcar-schema18-to19-server-transition-v1",
        )
        self.assertEqual(
            publisher.LEGACY_TRANSITION_SCHEMA,
            "dcar-schema17-to18-server-transition-v1",
        )
        self.assertEqual(len(publisher.RUNTIME_IDENTITY_KEYS), 10)

    def test_external_environment_rejects_symlink(self):
        link = self.root / "publisher-link.env"
        link.symlink_to(self.env_file)
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher._read_external_env(link, project_root=self.project)

    def test_external_environment_rejects_relative_snapshot_root(self):
        self.env_file.write_text(self.env_file.read_text().replace(str(self.snapshot_root), "relative"))
        with self.assertRaises(publisher.SnapshotPublishError):
            self.config()

    def test_wrapper_and_example_keep_scheduler_off_and_credentials_external(self):
        wrapper = (MACOS_DEPLOY / "run_snapshot_publisher.sh").read_text()
        example = (MACOS_DEPLOY / "publisher.env.example").read_text()
        self.assertTrue(os.access(MACOS_DEPLOY / "run_snapshot_publisher.sh", os.X_OK))
        self.assertIn('"${DCAR_READ_ONLY:-}" == "1"', wrapper)
        self.assertIn('"${DCAR_SCHEDULER_ENABLED:-}" == "0"', wrapper)
        self.assertIn('"${DCAR_STARTUP_CATCHUP_ENABLED:-}" == "0"', wrapper)
        self.assertIn("/usr/bin/caffeinate -i", wrapper)
        self.assertIn("arguments+=(--automatic)", wrapper)
        self.assertIn("arguments+=(--remote-check)", wrapper)
        self.assertIn('arguments+=(--resume-staged-snapshot "$snapshot_id")', wrapper)
        self.assertIn("^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$", wrapper)
        self.assertIn('publisher_intent="formal_read"', wrapper)
        self.assertIn('publisher_intent="remote_only"', wrapper)
        self.assertIn('publisher_intent="sealed_resume"', wrapper)
        self.assertIn("unset DCAR_V8_DB DCAR_LEGACY_DB DCAR_WRITER_LOCK", wrapper)
        self.assertLess(
            wrapper.index('publisher_intent=""'),
            wrapper.index("formal writer database is missing or unsafe"),
        )
        self.assertIn("unset SSH_AUTH_SOCK", wrapper)
        self.assertIn("TIKHUB_API_KEY_FILE", wrapper)
        self.assertIn("writer database must stay outside the repository", wrapper)
        self.assertIn("legacy database must stay outside the repository", wrapper)
        self.assertIn("DCAR_PUBLISH_EXPECTED_USER_VERSION=19", example)
        self.assertNotIn("TIKHUB_API_KEY=", example)
        self.assertNotIn("PASSWORD=", example)
        self.assertNotIn("PRIVATE_KEY=", example)
        self.assertIn("40,739,188,944", example)

    def test_python_remote_and_resume_modes_do_not_require_a_local_database(self):
        base = [
            "--project-root",
            str(self.project),
            "--env-file",
            str(self.env_file),
        ]
        remote = publisher._parser().parse_args([*base, "--remote-check"])
        resume = publisher._parser().parse_args(
            [*base, "--resume-staged-snapshot", "20260829T010000Z-" + "a" * 12]
        )
        self.assertIsNone(remote.db)
        self.assertIsNone(resume.db)

    def test_formal_read_observes_installed_lock_without_flock(self):
        with patch.object(
            publisher.runtime_database.fcntl,
            "flock",
            side_effect=AssertionError("formal_read must not attempt writer flock"),
        ):
            observation = publisher._observe_formal_read_source(
                self.database,
                project_root=self.project,
                now=self.now,
                config=self.config(),
                fetch_json=self.fixture.fetch,
            )
        self.assertTrue(observation.writer_lock["held"])
        self.assertEqual(
            observation.writer_lock["inode"], self.fixture.writer_lock.stat().st_ino
        )

    def test_remote_check_is_read_only_and_uses_no_local_state(self):
        runner = self.runner()
        calls = []

        def recording_runner(arguments, **kwargs):
            calls.append((list(arguments), dict(kwargs)))
            return runner(arguments, **kwargs)

        with patch.object(publisher.Path, "home", return_value=self.fake_home):
            result = publisher.remote_check(self.config(), runner=recording_runner)
        self.assertTrue(result["no_remote_write"])
        self.assertEqual(result["status"], "remote-check-ok")
        self.assertFalse(self.snapshot_root.exists())
        rendered = "\n".join(" ".join(command) for command in runner.commands)
        self.assertIn("IdentityAgent=none", rendered)
        self.assertIn(" --help", rendered)
        for forbidden in (" prune ", " install ", " rollback ", "rsync"):
            self.assertNotIn(forbidden, rendered)
        probe_arguments, probe_kwargs = next(
            call
            for call in calls
            if publisher.REMOTE_PROBE_SCHEMA in " ".join(call[0])
        )
        self.assertIn(
            f"timeout={publisher.REMOTE_ENDPOINT_TIMEOUT_SECONDS}",
            " ".join(probe_arguments),
        )
        self.assertEqual(
            probe_kwargs["timeout"], publisher.REMOTE_PROBE_COMMAND_TIMEOUT_SECONDS
        )
        self.assertEqual(publisher.REMOTE_ENDPOINT_TIMEOUT_SECONDS, 120)
        self.assertEqual(publisher.REMOTE_PROBE_COMMAND_TIMEOUT_SECONDS, 420)

    def test_schema_upgrade_marker_unsettled_blocks_before_build_or_staging(self):
        for index, status in enumerate(("in_progress", "rolling_back", "rollback_failed", "unknown")):
            runner = self.runner()
            runner.transition = {"schema": publisher.TRANSITION_SCHEMA, "status": status}
            with self.subTest(status=status), self.assertRaisesRegex(publisher.SnapshotPublishError, "unsettled"):
                self.publish(
                    runner=runner,
                    builder=self.builder,
                    now=self.now + timedelta(seconds=index),
                )
            self.assertFalse(any(command[0] == "rsync" for command in runner.commands))
            output = self.snapshot_root / (
                "snapshot-"
                + (self.now + timedelta(seconds=index)).astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            )
            self.assertTrue((output / publisher.SOURCE_RECEIPT_FILENAME).is_file())
            self.assertFalse(
                any(" install -d " in f" {' '.join(command)} " for command in runner.commands)
            )
        runner = self.runner()
        runner.transition = {"schema": publisher.TRANSITION_SCHEMA, "status": "succeeded", "completed_at": "2026-08-28T00:00:00Z",
                             "snapshot_id": "an-older-upgrade-snapshot"}
        value = publisher._validate_remote_probe(runner.probe(), config=self.config())
        self.assertEqual(value["schema_transition"], runner.transition)

        for transition_schema in (
            publisher.LEGACY_TRANSITION_SCHEMA,
            "dcar-schema19-to20-server-transition-v1",
        ):
            runner = self.runner()
            runner.transition = {
                "schema": transition_schema,
                "status": "succeeded",
                "completed_at": "2026-08-28T00:00:00Z",
            }
            with self.subTest(transition_schema=transition_schema), self.assertRaisesRegex(
                publisher.SnapshotPublishError, "unsettled"
            ):
                publisher._validate_remote_probe(runner.probe(), config=self.config())

    def test_normal_publisher_cannot_bootstrap_a_pre_schema19_database(self):
        for version in (16, 17, 18):
            value = self.runner().probe()
            value["health"]["database_state"]["user_version"] = version
            with self.subTest(version=version), self.assertRaisesRegex(publisher.SnapshotPublishError, "schema version"):
                publisher._validate_remote_probe(value, config=self.config())

    def test_remote_contract_and_readonly_guards_fail_closed(self):
        cases = []
        value = self.runner().probe()
        del value["schema_transition"]
        cases.append(value)
        value = self.runner().probe()
        value["health"]["snapshot_contract"]["artifact_policy"] = "thin-server-v1"
        cases.append(value)
        value = self.runner().probe()
        value["health"]["read_only"] = False
        cases.append(value)
        value = self.runner().probe()
        value["scheduler"]["enabled"] = True
        cases.append(value)
        for value in cases:
            with self.subTest(value=value), self.assertRaises(publisher.SnapshotPublishError):
                publisher._validate_remote_probe(value, config=self.config())

    def test_writer_freshness_accepts_today_partial_capture_but_rejects_stale_content(self):
        """Old capture/timestamp gate is replaced by current frozen scan evidence.

        The independent regression deliberately retains the former case name:
        a partial report is publishable; stale publication time is not a fault;
        failed/skipped scan flags contradicting immutable receipts are faults.
        """
        value = self.freshness()
        self.assertEqual(value.daily_report_status, "partial")
        self.assertIsNone(value.weekly_report_status)
        self.assertEqual(value.evidence["discovery"]["coverage"]["matrix_complete_windows"], 60)
        self.assertEqual(value.latest_published_at, "2026-08-28T04:00:00Z")
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE content_items SET published_at='2026-01-01T12:00:00Z'")
        self.assertEqual(self.freshness().latest_published_at, "2026-01-01T12:00:00Z")
        for status in ("failed", "skipped"):
            with self.subTest(status=status), sqlite3.connect(self.database) as connection:
                connection.execute("UPDATE scheduler_runs SET status=? WHERE id=?", (status, self.fixture.scan_ids[0]))
            with self.assertRaisesRegex(
                publisher.SnapshotPublishError,
                "publication_runtime_receipt_lineage_invalid",
            ):
                self.freshness()
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE scheduler_runs SET status='succeeded' WHERE id=?", (self.fixture.scan_ids[0],))
        self.assertEqual(self.freshness().daily_report_status, "partial")

    def test_writer_freshness_requires_successful_cutoff_and_held_writer_lock(self):
        """07:30 preparation replaces legacy media-cutoff, without fake success."""
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE scheduler_runs SET status='failed' WHERE job_id='daily_pipeline_summary'")
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "publication_run_not_terminal"):
            self.freshness()
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE scheduler_runs SET status='succeeded' WHERE job_id='daily_pipeline_summary'")
        for change in ({"writer_lock": {"held": False}}, {"enabled": False},
                       {"startup_catchup": {"mode": "all", "requested": True, "enabled": True, "status": "succeeded", "results": []}}):
            def fetch(url):
                value = self.fixture.fetch(url)
                return {**value, **change} if url.endswith("/scheduler") else value
            with self.subTest(change=change), self.assertRaises(publisher.SnapshotPublishError):
                publisher.check_writer_freshness(self.database, now=self.now, maximum_content_lag_days=1, fetch_json=fetch, project_root=self.project)

    def test_writer_freshness_requires_media_and_report_terminal_chain(self):
        """Media absence stays partial; missing raw/frozen evidence cannot pass.

        Original three failure branches remain independent, but the new
        contract binds real raw/download evidence and frozen analysis inputs,
        rather than requiring invented all-green legacy media cron rows.
        """
        for stage in ("download_evidence", "processing_inputs", "daily_report"):
            with self.subTest(stage=stage):
                fixture = create_writer_fixture(self.root / stage)
                self.assertEqual(self.freshness(fixture).daily_report_status, "partial")
                with sqlite3.connect(fixture.database) as connection:
                    if stage == "download_evidence":
                        path = connection.execute("SELECT local_path FROM provider_raw_responses WHERE operation='matrix_works_scan' ORDER BY id LIMIT 1").fetchone()
                        if path is None:
                            path = connection.execute("SELECT local_path FROM provider_raw_responses WHERE provider='newrank_matrix' AND operation != 'matrix_roster_candidate' ORDER BY id LIMIT 1").fetchone()
                        self.assertIsNotNone(path)
                        Path(path[0]).unlink()
                    elif stage == "processing_inputs":
                        connection.execute("UPDATE task_events SET payload_json='{}' WHERE event_type='report_inputs_v1'")
                    else:
                        connection.execute("UPDATE scheduler_runs SET status='failed' WHERE job_id='daily_report'")
                with self.assertRaises(publisher.SnapshotPublishError):
                    self.freshness(fixture)

    def test_writer_freshness_rejects_stale_downstream_completion_order(self):
        """Exact cutoff/attempt ordering replaces legacy job-name sequencing."""
        late = create_writer_fixture(self.root / "late-chain", late_scan=True)
        value = self.freshness(late)
        self.assertEqual(value.evidence["status"], "partial")
        self.assertFalse(value.evidence["reports"][0]["partial_reasons"]["discovery_coverage"]["complete"])
        with sqlite3.connect(late.database) as connection:
            connection.row_factory = sqlite3.Row
            with self.assertRaisesRegex(
                pipeline_cutover.PublicationEvidenceError,
                "publication_scan_lineage_invalid",
            ):
                pipeline_cutover.verify_scan_reference(connection, late.scan_ids[2], at="2026-08-29T00:00:00Z")
        for job_id in ("daily_pipeline_summary", "daily_report"):
            with self.subTest(job_id=job_id):
                database = self.copy_database(job_id + "-future.sqlite3")
                with sqlite3.connect(database) as connection:
                    connection.execute("UPDATE scheduler_runs SET completed_at='2026-08-29T02:00:00Z' WHERE job_id=?", (job_id,))
                def fetch(url):
                    result = copy.deepcopy(self.fixture.fetch(url))
                    if url.endswith("/health"):
                        result.update(
                            database=database.name,
                            runtime_database_identity=self.runtime_database_identity(database),
                        )
                    return result
                expected_error = "daily_report completion time is not current" if job_id == "daily_report" else "publication_run_time_invalid"
                with self.assertRaisesRegex(publisher.SnapshotPublishError, expected_error):
                    publisher.check_writer_freshness(database, now=self.now, maximum_content_lag_days=1,
                        fetch_json=fetch, project_root=self.project)

    def test_writer_freshness_requires_monday_weekly_report(self):
        fixture = create_writer_fixture(self.root / "monday", day=date(2026, 8, 31))
        with sqlite3.connect(fixture.database) as connection:
            connection.execute("UPDATE scheduler_runs SET status='failed' WHERE job_id='weekly_report'")
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "weekly_report status is not publishable: failed"):
            self.freshness(fixture)
        with sqlite3.connect(fixture.database) as connection:
            connection.execute("UPDATE scheduler_runs SET status='partial' WHERE job_id='weekly_report'")
        result = self.freshness(fixture)
        self.assertEqual(result.weekly_report_status, "partial")
        self.assertEqual(len(result.evidence["reports"]), 2)
        self.assertEqual(result.evidence["reports"][1]["period_start"], "2026-08-24")
        self.assertEqual(result.evidence["reports"][1]["cutoff_at"], "2026-08-31T00:30:00Z")
        with sqlite3.connect(fixture.database) as connection:
            connection.execute("UPDATE scheduler_runs SET completed_at='2026-08-31T02:00:00Z' WHERE job_id='weekly_report'")
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "weekly_report completion time is not current"):
            self.freshness(fixture)

    def test_writer_freshness_uses_terminal_reports_when_startup_catchup_is_stale(self):
        for change in ({"status": "running"}, {"enabled": False, "requested": False, "status": "disabled"},
                       {"results": [{"job_id": "daily_report", "status": "deferred", "scheduled_for": "2026-08-29T00:00:00Z"}]},
                       {"results": [{"job_id": "daily_report", "status": "failed", "scheduled_for": "2026-08-29T00:00:00Z"}]}):
            def fetch(url):
                value = self.fixture.fetch(url)
                if url.endswith("/scheduler"):
                    value["startup_catchup"].update(change)
                return value
            with self.subTest(change=change):
                self.assertEqual(self.freshness(fetch=fetch).daily_report_status, "partial")

    def test_writer_freshness_rejects_non_report_startup_work(self):
        def fetch(url):
            value = self.fixture.fetch(url)
            if url.endswith("/scheduler"):
                value["startup_catchup"]["results"] = [{"job_id": "daily_capture"}]
            return value
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "non-report work"):
            self.freshness(fetch=fetch)

    def test_recovered_report_only_catchup_does_not_hide_terminal_report(self):
        for status in ("failed", "deferred"):
            def fetch(url):
                value = self.fixture.fetch(url)
                if url.endswith("/scheduler"):
                    value["startup_catchup"].update(status=status, results=[{
                        "job_id": "daily_report", "status": status,
                        "scheduled_for": "2026-08-29T00:00:00Z"}])
                return value
            value = publisher.check_writer_freshness(self.database, now=self.now, maximum_content_lag_days=1, fetch_json=fetch, project_root=self.project)
            self.assertEqual(value.daily_report_status, "partial")

    def test_old_catchup_success_does_not_replace_actual_partial_report(self):
        def fetch(url):
            value = self.fixture.fetch(url)
            if url.endswith("/scheduler"):
                value["startup_catchup"]["results"] = [{"job_id": "daily_report", "status": "succeeded",
                    "scheduled_for": "2026-08-29T00:00:00Z"}]
            return value
        self.assertEqual(self.freshness(fetch=fetch).daily_report_status, "partial")

    def test_writer_freshness_rejects_health_or_database_identity_drift(self):
        def fetch(url):
            value = copy.deepcopy(self.fixture.fetch(url))
            if url.endswith("/health"):
                value["database_state"]["runtime_identity"]["matcher_rule_sha256"] = "e" * 64
            return value
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "does not match the formal database"):
            publisher.check_writer_freshness(self.database, now=self.now, maximum_content_lag_days=1, fetch_json=fetch, project_root=self.project)
        wrong_release = self.copy_database("wrong-release.sqlite3")
        with sqlite3.connect(wrong_release) as connection:
            connection.execute("UPDATE evaluation_releases SET id='evaluation-v8__selling-points-v5.2',rule_version='evaluation-v8' WHERE status='active'")
        def wrong_release_health(url):
            value = copy.deepcopy(self.fixture.fetch(url))
            if url.endswith("/health"):
                value["database"] = wrong_release.name
                value["runtime_database_identity"] = self.runtime_database_identity(wrong_release)
                value["database_state"]["runtime_identity"].update(active_release_id="evaluation-v8__selling-points-v5.2", rule_version="evaluation-v8")
            return value
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "active_release_id"):
            publisher.check_writer_freshness(wrong_release, now=self.now, maximum_content_lag_days=1,
                fetch_json=wrong_release_health, project_root=self.project)

    def test_writer_database_version_and_migration_are_verified_not_inferred_from_health(self):
        cases = (
            ("schema16", "PRAGMA user_version=16"),
            ("schema17", "PRAGMA user_version=17"),
            ("schema18", "PRAGMA user_version=18"),
            (
                "migration",
                "UPDATE schema_migrations SET name='unexpected' WHERE version=19",
            ),
        )
        for name, sql in cases:
            with self.subTest(name=name):
                database = self.copy_database(name + ".sqlite3")
                with sqlite3.connect(database) as connection:
                    connection.execute(sql)
                def fetch(url):
                    value = copy.deepcopy(self.fixture.fetch(url))
                    if url.endswith("/health"):
                        value.update(
                            database=database.name,
                            runtime_database_identity=self.runtime_database_identity(database),
                        )
                    return value
                before = database.read_bytes()
                with self.assertRaises(publisher.SnapshotPublishError):
                    publisher.check_writer_freshness(database, now=self.now, maximum_content_lag_days=1,
                        fetch_json=fetch, project_root=self.project)
                self.assertEqual(database.read_bytes(), before)

    def test_database_symlinks_are_rejected_before_network_or_snapshot(self):
        link = self.root / "database-link.sqlite3"
        link.symlink_to(self.database)
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher.check_writer_freshness(link, maximum_content_lag_days=1,
                fetch_json=lambda _: self.fail("network must not run"))
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher.publish_snapshot(project_root=self.project, database=self.database, legacy_database=link,
                config=self.config(), runner=lambda *_a, **_k: self.fail("network must not run"))

    def test_rsync_stats_parser_accepts_macos_openrsync_byte_unit(self):
        result = publisher._rsync_transfer_bytes(lambda arguments, **_: subprocess.CompletedProcess(
            arguments, 0, stdout="Total transferred file size: 1234 B\n", stderr=""),
            ["rsync", "source/", "destination/"], timeout=1)
        self.assertEqual(result, 1234)

    def test_automatic_publish_is_noop_before_0900_without_network_or_snapshot(self):
        runner = self.runner()
        result = self.publish(runner=runner, automatic=True, now=self.now - timedelta(minutes=1),
            fetch=lambda _: self.fail("writer must not be queried"),
            builder=lambda **_: self.fail("no build before 09:00"))
        self.assertEqual(result["status"], "before-automatic-window")
        self.assertEqual(runner.commands, [])
        self.assertFalse(self.snapshot_root.exists())

    def test_forward_first_day_publishes_collection_and_later_content_changes(self):
        def fetch(url):
            value = self.fixture.fetch(url)
            if url.endswith("/scheduler"):
                value["reconcile_from"] = DAY.isoformat()
                value["startup_catchup"] = {"enabled": False, "requested": False, "status": "disabled"}
            return value
        runner = self.runner()
        def progressing_builder(**arguments):
            with sqlite3.connect(self.database) as connection:
                connection.execute("UPDATE content_items SET published_at='2026-08-29T00:55:00Z',updated_at='2026-08-29T00:55:00Z'")
            return self.builder(**arguments)
        first = self.publish(runner=runner, automatic=True, fetch=fetch, builder=progressing_builder)
        evidence = first["publication_evidence"]
        self.assertEqual(evidence["mode"], "observed")
        self.assertEqual(evidence["reports"], [])
        self.assertEqual(evidence["status"], "partial")
        self.assertIn("first_report_not_due", evidence["reasons"])
        self.assertGreater(evidence["current_observation"]["content"]["row_count"], 0)
        self.assertEqual(evidence["first_daily_report_due_at"], "2026-08-30T08:00:00+08:00")
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE content_items SET published_at='2026-08-29T01:15:00Z',updated_at='2026-08-29T01:15:00Z'")
        later = self.publish(runner=runner, automatic=True, fetch=fetch, now=self.now + timedelta(hours=1))
        self.assertNotEqual(first["snapshot_id"], later["snapshot_id"])
        self.assertGreater(later["publication_evidence"]["current_observation"]["content"]["row_count"], 0)
        count = len(runner.commands)
        unchanged = self.publish(runner=runner, automatic=True, fetch=fetch, now=self.now + timedelta(hours=1, minutes=15),
                                 builder=lambda **_: self.fail("unchanged forward observations must not rebuild"))
        self.assertEqual(unchanged["status"], "already-published-current-evidence")
        self.assertEqual(len(runner.commands), count)

    def test_forward_scope_does_not_require_a_week_crossing_the_boundary(self):
        fixture = create_writer_fixture(self.root / "forward-monday", day=date(2026, 8, 31))
        with sqlite3.connect(fixture.database) as connection:
            connection.execute("UPDATE scheduler_runs SET status='failed' WHERE job_id='weekly_report'")
        def fetch(url):
            value = fixture.fetch(url)
            if url.endswith("/scheduler"):
                value["reconcile_from"] = "2026-08-30"
            return value
        value = publisher.check_writer_freshness(fixture.database, now=fixture.now, maximum_content_lag_days=1,
                                                fetch_json=fetch, project_root=fixture.project)
        self.assertEqual([row["job_id"] for row in value.evidence["report_observations"]], ["daily_report"])
        self.assertIsNone(value.weekly_report_status)

    def test_forward_missing_and_genuinely_failed_report_remain_visible(self):
        fixture = create_writer_fixture(self.root / "forward-failed", report=False)
        def fetch(url):
            value = fixture.fetch(url)
            if url.endswith("/scheduler"):
                value["reconcile_from"] = (DAY - timedelta(days=1)).isoformat()
            return value
        def observe():
            return publisher.check_writer_freshness(fixture.database, now=fixture.now, maximum_content_lag_days=1,
                                                   fetch_json=fetch, project_root=fixture.project).evidence
        self.assertIn("daily_report_missing", observe()["reasons"])
        encoded = json.dumps({"reason": "fixture_report_failure"})
        with sqlite3.connect(fixture.database) as connection:
            run_id = connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
                "VALUES ('daily_report','2026-08-29T08:00:00+08:00','failed','2026-08-29T00:00:00Z','2026-08-29T00:01:00Z',?)",
                (encoded,),
            ).lastrowid
            connection.execute(
                "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,status,started_at,completed_at,details_json) "
                "VALUES (?,1,'scheduled','failed','2026-08-29T00:00:00Z','2026-08-29T00:01:00Z',?)", (run_id, encoded),
            )
        evidence = observe()
        self.assertEqual(evidence["status"], "partial")
        self.assertIn("daily_report_failed", evidence["reasons"])
        self.assertEqual(evidence["reports"], [])
        with sqlite3.connect(fixture.database) as connection:
            connection.execute("UPDATE scheduler_runs SET status='succeeded' WHERE id=?", (run_id,))
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "publication_attempt_mismatch"):
            observe()

    def test_previous_day_runtime_state_does_not_block_a_new_release(self):
        receipt = self.publish(automatic=True)
        path = self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME
        state = json.loads(path.read_text())
        state["runtime_identity"]["report_version"] = "previous-release-report"
        path.write_text(json.dumps(state))
        self.assertIsNone(publisher._read_automatic_state(self.snapshot_root, beijing_date=DAY + timedelta(days=1)))
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher._read_automatic_state(self.snapshot_root, beijing_date=DAY)
        self.assertEqual(receipt["publication_status"], "partial")

    def test_publisher_status_records_refusal_without_claiming_publication(self):
        publisher._record_publisher_status(self.config(), {"status": "blocked", "reason": "fixture integrity refusal"})
        path = self.snapshot_root / "publisher-status.json"
        state = json.loads(path.read_text())
        self.assertEqual(state["reason"], "fixture integrity refusal")
        self.assertNotIn("published_at", state)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        publisher._record_publisher_status(self.config(), {
            "snapshot_id": "fixture", "published_at": "2026-08-29T01:00:00Z",
            "publication_status": "partial", "publication_evidence": {
                "reasons": ["first_report_not_due"], "reconcile_from": DAY.isoformat(),
            },
        })
        state = json.loads(path.read_text())
        self.assertEqual(state["status"], "published")
        self.assertEqual(state["publication_status"], "partial")
        self.assertEqual(state["publication_reasons"], ["first_report_not_due"])

    def test_automatic_publish_not_ready_stops_before_ssh_or_snapshot(self):
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE scheduler_runs SET status='running',completed_at=NULL WHERE job_id='daily_pipeline_summary'")
        runner = self.runner()
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "not_terminal"):
            self.publish(runner=runner, automatic=True, builder=lambda **_: self.fail("not ready"))
        self.assertEqual(runner.commands, [])

    def test_automatic_publish_writes_atomic_daily_state_and_deduplicates(self):
        runner = self.runner()
        receipt = self.publish(runner=runner, automatic=True)
        state_path = self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME
        state = json.loads(state_path.read_bytes())
        self.assertEqual(state_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(state["beijing_date"], DAY.isoformat())
        self.assertEqual(state["publication_evidence_sha256"], receipt["publication_evidence_sha256"])
        count = len(runner.commands)
        result = self.publish(runner=runner, automatic=True, now=self.now + timedelta(minutes=15),
            builder=lambda **_: self.fail("unchanged evidence must not rebuild"))
        self.assertEqual(result["status"], "already-published-current-evidence")
        self.assertEqual(len(runner.commands), count)

    def test_unchanged_evidence_renews_through_full_publish_at_dedup_expiry(self):
        runner = self.runner()
        receipt = self.publish(runner=runner, automatic=True)
        count = len(runner.commands)
        def build_new_snapshot_identity(**arguments):
            manifest = self.builder(**arguments)
            manifest["snapshot_id"] = "20260829T013000Z-" + manifest["databases"][0]["sha256"][:12]
            self.write_manifest(arguments["output"], manifest)
            return manifest
        result = self.publish(runner=runner, automatic=True,
            now=self.now + timedelta(seconds=publisher.AUTOMATIC_DEDUP_SECONDS),
            builder=build_new_snapshot_identity)
        self.assertGreater(len(runner.commands), count)
        self.assertTrue(any(" install --bundle " in " " + " ".join(command) + " "
            for command in runner.commands[count:]))
        self.assertGreater(publisher._parse_iso(result["published_at"], label="renewal"),
            publisher._parse_iso(receipt["published_at"], label="previous"))
        self.assertNotIn("no_ssh_attempted", result)

    def test_failed_hourly_renewal_does_not_refresh_success_time(self):
        self.publish(automatic=True)
        state_path = self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME
        before = state_path.read_bytes()
        receipt_path = self.snapshot_root / json.loads(before)["output_name"] / "publisher-receipt.json"
        receipt_before = receipt_path.read_bytes()
        with self.assertRaises(publisher.SnapshotPublishError):
            self.publish(automatic=True, now=self.now + timedelta(hours=1), runner=self.runner(fail_install=True))
        self.assertEqual(state_path.read_bytes(), before)
        self.assertEqual(receipt_path.read_bytes(), receipt_before)

    def test_malformed_success_time_cannot_refresh_or_shortcut_publication(self):
        self.publish(automatic=True)
        state_path = self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME
        before = state_path.read_bytes()
        receipt_path = self.snapshot_root / json.loads(before)["output_name"] / "publisher-receipt.json"
        receipt = json.loads(receipt_path.read_text())
        for malformed in ("not-a-time", "2026-08-29T01:00:00", None):
            with self.subTest(published_at=malformed):
                receipt["published_at"] = malformed
                publisher._write_json_atomic(receipt_path, receipt)
                runner = self.runner()
                with self.assertRaises(publisher.SnapshotPublishError):
                    self.publish(automatic=True, runner=runner,
                        builder=lambda **_: self.fail("bad timestamp rebuilt"))
                self.assertEqual(runner.commands, [])
                self.assertEqual(state_path.read_bytes(), before)

    def test_future_success_time_refuses_deduplication_and_publish(self):
        self.publish(automatic=True, now=self.now + timedelta(minutes=30))
        state_path = self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME
        before = state_path.read_bytes()
        runner = self.runner()
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "in the future"):
            self.publish(automatic=True, runner=runner, builder=lambda **_: self.fail("future receipt rebuilt"))
        self.assertEqual(runner.commands, [])
        self.assertEqual(state_path.read_bytes(), before)

    def test_automatic_publish_recovers_state_from_success_receipt(self):
        runner = self.runner()
        self.publish(runner=runner, automatic=True)
        (self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME).unlink()
        count = len(runner.commands)
        result = self.publish(runner=runner, automatic=True, builder=lambda **_: self.fail("no rebuild"))
        self.assertEqual(result["status"], "already-published-current-evidence")
        self.assertEqual(len(runner.commands), count)

    def test_automatic_success_does_not_trust_a_changed_or_missing_receipt(self):
        self.publish(automatic=True)
        path = next(self.snapshot_root.glob("snapshot-*/publisher-receipt.json"))
        receipt = json.loads(path.read_bytes())
        receipt["publication_status"] = "succeeded"
        path.write_text(json.dumps(receipt))
        with self.assertRaises(publisher.SnapshotPublishError):
            self.publish(automatic=True, fetch=lambda _: self.fail("no writer query"))

    def test_pending_new_remote_completes_without_rebuild(self):
        runner = self.create_pending()
        result = self.publish(runner=runner, automatic=True,
            fetch=lambda _: self.fail("pending reconcile queried writer"), builder=lambda **_: self.fail("pending reconcile rebuilt"))
        self.assertEqual(result["snapshot_id"], runner.active_snapshot_id)
        self.assertFalse((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())
        self.assertTrue((self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME).exists())

    def test_pending_previous_remote_clears_for_retry(self):
        runner = self.create_pending()
        runner.active_snapshot_id = "20260828T010000Z-" + "b" * 12
        runner.active_database_sha256 = "b" * 64
        runner.active_manifest_sha256 = "b" * 64
        with patch.object(publisher.Path, "home", return_value=self.fake_home):
            ssh = publisher._check_ssh_alias(self.config(), runner=runner)
            value = publisher._reconcile_pending_publish(self.config(), ssh, runner=runner)
        self.assertIsNone(value)
        self.assertFalse((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())

    def test_pending_unknown_remote_blocks_and_is_retained(self):
        runner = self.create_pending()
        runner.active_database_sha256 = "c" * 64
        with patch.object(publisher.Path, "home", return_value=self.fake_home), self.assertRaisesRegex(publisher.SnapshotPublishError, "matches neither"):
            ssh = publisher._check_ssh_alias(self.config(), runner=runner)
            publisher._reconcile_pending_publish(self.config(), ssh, runner=runner)
        self.assertTrue((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())

    def test_tampered_pending_payload_is_not_reconciled(self):
        self.create_pending()
        path = self.snapshot_root / publisher.PENDING_STATE_FILENAME
        value = json.loads(path.read_bytes())
        value["receipt"]["publication_evidence"]["reports"][0]["input_sha256"] = "d" * 64
        path.write_text(json.dumps(value))
        runner = self.runner()
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "pending receipt is invalid"):
            self.publish(runner=runner, automatic=True)
        self.assertEqual(runner.commands, [])

    def test_legacy_pending_blocks_and_is_preserved(self):
        self.snapshot_root.mkdir()
        path = self.snapshot_root / publisher.PENDING_STATE_FILENAME
        publisher._write_json_atomic(path, {"schema": "dcar-snapshot-publisher-pending-v1", "status": "succeeded"})
        before = path.read_bytes()
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "legacy pending"):
            self.publish(automatic=True, fetch=lambda _: self.fail("no writer query"))
        self.assertEqual(path.read_bytes(), before)

    def test_automatic_state_recovery_ignores_old_unpublishable_receipt(self):
        self.snapshot_root.mkdir()
        output = self.snapshot_root / "snapshot-20260829T010000Z"
        output.mkdir()
        path = output / "publisher-receipt.json"
        path.write_text(json.dumps({"schema": "dcar-snapshot-publisher-receipt-v1", "capture_status": "failed",
            "media_download_status": "succeeded", "media_processing_status": "succeeded", "media_cutoff_status": "succeeded",
            "daily_report_status": "partial", "snapshot_id": "20260829T010000Z-" + "a" * 12,
            "published_at": "2026-08-29T01:01:00Z", "database_sha256": "a" * 64}))
        before = path.read_bytes()
        self.assertIsNone(publisher._recover_automatic_state(self.snapshot_root, beijing_date=DAY))
        self.assertEqual(path.read_bytes(), before)
        self.assertFalse((self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME).exists())

    def test_automatic_state_recovery_ignores_old_successful_capture_receipt(self):
        self.snapshot_root.mkdir()
        output = self.snapshot_root / "snapshot-20260829T010000Z"
        output.mkdir()
        path = output / "publisher-receipt.json"
        path.write_text(json.dumps({"schema": "dcar-snapshot-publisher-receipt-v1", "capture_status": "succeeded",
            "media_cutoff_status": "succeeded", "daily_report_status": "succeeded",
            "snapshot_id": "20260829T010000Z-" + "a" * 12, "database_sha256": "a" * 64}))
        before = path.read_bytes()
        self.assertIsNone(publisher._recover_automatic_state(self.snapshot_root, beijing_date=DAY))
        self.assertEqual(path.read_bytes(), before)
        self.assertFalse((self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME).exists())

    def test_local_snapshot_retention_keeps_latest_three_and_ignores_unsafe(self):
        self.snapshot_root.mkdir()
        names = [f"snapshot-2026081{day}T010000Z" for day in range(1, 6)]
        for name in names:
            (self.snapshot_root / name).mkdir()
        outside = self.root / "outside"
        outside.mkdir()
        link = self.snapshot_root / "snapshot-20260810T010000Z"
        link.symlink_to(outside, target_is_directory=True)
        (self.snapshot_root / "not-a-snapshot").mkdir()
        self.assertEqual(publisher._prune_local_snapshots(self.snapshot_root), names[:2])
        self.assertEqual({path.name for path in self.snapshot_root.iterdir() if path.is_dir() and not path.is_symlink()},
            set(names[2:]) | {"not-a-snapshot"})
        self.assertTrue(link.is_symlink())
        self.assertTrue(outside.is_dir())
        self.assertTrue((self.snapshot_root / "not-a-snapshot").exists())

    def test_failed_automatic_publish_keeps_local_snapshot_count_bounded(self):
        self.snapshot_root.mkdir()
        for day in range(1, 5):
            (self.snapshot_root / f"snapshot-2026080{day}T010000Z").mkdir()
        with self.assertRaises(publisher.SnapshotPublishError):
            self.publish(runner=self.runner(fail_install=True), automatic=True)
        self.assertLessEqual(len(list(self.snapshot_root.glob("snapshot-*"))), 3)

    def test_repeated_failed_rebuilds_preserve_the_automatic_success_receipt(self):
        self.publish(automatic=True)
        state_path = self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME
        original_state = state_path.read_bytes()
        state = json.loads(original_state)
        receipt_path = self.snapshot_root / state["output_name"] / "publisher-receipt.json"
        original_receipt = receipt_path.read_bytes()

        def fail_after_creating_bundle(**arguments):
            Path(arguments["output"]).mkdir()
            raise publisher.SnapshotPublishError("fixture transfer/build interrupted")

        for hour in range(1, 5):
            with patch.object(publisher, "_publication_fingerprint", side_effect=lambda _: object()):
                with self.assertRaisesRegex(publisher.SnapshotPublishError, "fixture transfer/build"):
                    self.publish(automatic=True, now=self.now + timedelta(hours=hour),
                        builder=fail_after_creating_bundle)
            self.assertEqual(receipt_path.read_bytes(), original_receipt)
            self.assertEqual(state_path.read_bytes(), original_state)
            self.assertLessEqual(len(list(self.snapshot_root.glob("snapshot-*"))), 3)

        self.assertEqual(publisher._daily_automatic_success(self.snapshot_root, beijing_date=DAY), state)
        self.publish(automatic=True, now=self.now + timedelta(hours=5))
        self.assertTrue(self.last_runner.install_attempted)

    def test_retention_preserves_both_success_and_pending_within_limit(self):
        self.publish(automatic=True)
        success = json.loads((self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME).read_text())
        with patch.object(publisher, "_publication_fingerprint", side_effect=lambda _: object()):
            with self.assertRaisesRegex(publisher.SnapshotPublishError, "temporarily unavailable"):
                self.publish(automatic=True, now=self.now + timedelta(hours=1),
                    runner=self.runner(fail_probe_after_install=True))
        pending = publisher._read_pending_state(self.snapshot_root)
        later = [f"snapshot-20260829T1{hour}0000Z" for hour in range(3)]
        for name in later:
            (self.snapshot_root / name).mkdir()

        publisher._prune_local_snapshots(self.snapshot_root, retain_count=2)
        self.assertEqual({path.name for path in self.snapshot_root.glob("snapshot-*") if path.is_dir()},
            {success["output_name"], pending["output_name"]})
        self.assertEqual(publisher._read_pending_state(self.snapshot_root), pending)
        self.assertEqual(publisher._daily_automatic_success(self.snapshot_root, beijing_date=DAY), success)
        before = sorted(path.name for path in self.snapshot_root.iterdir())
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "exceed the retention limit"):
            publisher._prune_local_snapshots(self.snapshot_root, retain_count=1)
        self.assertEqual(sorted(path.name for path in self.snapshot_root.iterdir()), before)

    def test_finishing_older_resume_preserves_receipt_without_automatic_state(self):
        receipt = self.publish(automatic=True)
        state_path = self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME
        state = json.loads(state_path.read_text())
        state_path.unlink()
        for hour in range(4):
            (self.snapshot_root / f"snapshot-20260829T1{hour}0000Z").mkdir()
        pending = {"output_name": state["output_name"], "receipt": receipt, "beijing_date": None}
        result = publisher._finish_pending_publish(self.config(), pending)
        self.assertEqual(result, receipt)
        self.assertEqual(json.loads((self.snapshot_root / state["output_name"] / "publisher-receipt.json").read_text()), receipt)
        self.assertEqual(len(list(self.snapshot_root.glob("snapshot-*"))), 3)

    def test_retention_preserves_old_release_reference_without_authorizing_it(self):
        self.publish(automatic=True)
        state_path = self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME
        state = json.loads(state_path.read_text())
        state["runtime_identity"]["report_version"] = "previous-release-report"
        publisher._write_json_atomic(state_path, state)
        for hour in range(4):
            (self.snapshot_root / f"snapshot-20260829T1{hour}0000Z").mkdir()
        publisher._prune_local_snapshots(self.snapshot_root, retain_count=2)
        self.assertTrue((self.snapshot_root / state["output_name"] / "publisher-receipt.json").is_file())
        self.assertEqual(len(list(self.snapshot_root.glob("snapshot-*"))), 2)
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher._daily_automatic_success(self.snapshot_root, beijing_date=DAY)

    def test_unsafe_retention_reference_blocks_before_deleting_snapshots(self):
        self.snapshot_root.mkdir()
        names = [f"snapshot-2026081{day}T010000Z" for day in range(1, 6)]
        for name in names:
            (self.snapshot_root / name).mkdir()
        publisher._write_json_atomic(self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME,
            {"schema": publisher.AUTOMATIC_STATE_SCHEMA, "output_name": "../outside"})
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "retention reference is invalid"):
            publisher._prune_local_snapshots(self.snapshot_root)
        self.assertEqual(sorted(path.name for path in self.snapshot_root.glob("snapshot-*")), names)

    def test_resume_staged_snapshot_reuses_transport_and_seals_original_day(self):
        output, manifest, runner = self.staged_resume()

        receipt = self.publish(
            runner=runner,
            fetch=lambda _url: self.fail("sealed resume queried live writer health"),
            builder=lambda **_: self.fail("staged resume rebuilt the snapshot"),
            resume_staged_snapshot_id=manifest["snapshot_id"],
        )

        self.assertEqual(receipt["transport_mode"], "reused-complete-staging")
        self.assertEqual(
            receipt["required_remote_bytes"],
            self.config().minimum_remote_free_bytes
            + receipt["install_headroom"]["total_bytes"],
        )
        self.assertEqual(receipt["remote_free_bytes_after_dry_run"], runner.free_bytes)
        self.assertIsNone(receipt["remote_retention"])
        self.assertEqual(receipt["rsync_dry_run_cache_bytes"], 1024)
        self.assertEqual(receipt["rsync_dry_run_reports_bytes"], 1024)
        self.assertEqual(receipt["rsync_dry_run_bundle_bytes"], 0)
        self.assertEqual(receipt["rsync_dry_run_transfer_bytes"], 0)
        rendered = [" ".join(command) for command in runner.commands]
        self.assertEqual(sum("--dry-run" in command for command in rendered), 3)
        incoming = "/var/lib/dcar-aigc/incoming/" + manifest["snapshot_id"]
        self.assertTrue(
            any(
                f"{incoming}/artifacts/cache/" in command
                and "/var/lib/dcar-aigc/cache/" in command
                for command in rendered
            )
        )
        self.assertTrue(
            any(
                f"{incoming}/artifacts/reports/" in command
                and "/var/lib/dcar-aigc/reports/" in command
                for command in rendered
            )
        )
        artifact_dry_runs = [
            command
            for command in rendered
            if "--dry-run" in command and f"{incoming}/artifacts/" in command
        ]
        self.assertEqual(len(artifact_dry_runs), 2)
        self.assertTrue(
            all("--files-from=" not in command for command in artifact_dry_runs)
        )
        self.assertFalse(
            any(str(self.project / "data/cache") in command for command in rendered)
        )
        self.assertTrue(
            all(
                command[0] != "rsync" or "--dry-run" in command
                for command in runner.commands
            )
        )
        self.assertFalse(any(" install -d " in f" {command} " for command in rendered))
        self.assertFalse(any(" prune " in f" {command} " for command in rendered))
        verify = next(index for index, command in enumerate(rendered) if " verify --bundle " in f" {command} ")
        install = next(index for index, command in enumerate(rendered) if " install --bundle " in f" {command} ")
        self.assertLess(verify, install)
        state = json.loads((self.snapshot_root / publisher.AUTOMATIC_STATE_FILENAME).read_bytes())
        self.assertEqual(state["beijing_date"], DAY.isoformat())
        self.assertEqual(state["snapshot_id"], manifest["snapshot_id"])
        self.assertFalse((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())
        self.assertEqual(json.loads((output / "publisher-receipt.json").read_bytes()), receipt)

    def test_source_receipt_is_self_hashing_exclusive_and_precedes_ssh(self):
        runner = self.runner()

        def guarded_runner(arguments, **kwargs):
            receipts = list(
                self.snapshot_root.glob(
                    f"snapshot-*/{publisher.SOURCE_RECEIPT_FILENAME}"
                )
            )
            self.assertEqual(len(receipts), 1)
            return runner(arguments, **kwargs)

        receipt = self.publish(runner=guarded_runner)
        source_path = next(
            self.snapshot_root.glob(f"snapshot-*/{publisher.SOURCE_RECEIPT_FILENAME}")
        )
        source = json.loads(source_path.read_bytes())
        self.assertEqual(source_path.stat().st_mode & 0o777, 0o600)
        claimed = source.pop("payload_sha256")
        self.assertEqual(claimed, pipeline_cutover.digest(source))
        self.assertEqual(receipt["source_receipt_payload_sha256"], claimed)
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "overwrite"):
            publisher._write_json_exclusive(source_path, source)

    def test_resume_rejects_tampered_source_receipt_without_live_dependencies(self):
        output, manifest, runner = self.staged_resume()
        source_path = output / publisher.SOURCE_RECEIPT_FILENAME
        source = json.loads(source_path.read_bytes())
        source["content_count"] += 1
        source_path.write_text(json.dumps(source))
        source_path.chmod(0o600)
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "payload SHA-256"):
            self.publish(
                runner=runner,
                fetch=lambda _url: self.fail("tampered resume queried live writer"),
                builder=lambda **_: self.fail("tampered resume rebuilt"),
                resume_staged_snapshot_id=manifest["snapshot_id"],
            )
        self.assertEqual(runner.commands, [])

    def test_resume_rejects_invalid_or_ambiguous_local_snapshot_before_ssh(self):
        runner = self.runner()
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "snapshot_id is invalid"):
            self.publish(
                runner=runner,
                resume_staged_snapshot_id="../../arbitrary-staging",
            )
        self.assertEqual(runner.commands, [])

        first, manifest, _ = self.staged_resume(output_name="snapshot-20260829T010000Z")
        second = self.snapshot_root / "snapshot-20260829T010001Z"
        self.builder(
            project_root=self.project,
            database=self.database,
            legacy_database=None,
            output=second,
            expected_user_version=SCHEMA_VERSION,
        )
        runner = self.runner()
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "exactly one"):
            self.publish(
                runner=runner,
                builder=lambda **_: self.fail("ambiguous resume rebuilt"),
                resume_staged_snapshot_id=manifest["snapshot_id"],
            )
        self.assertTrue(first.is_dir() and second.is_dir())
        self.assertEqual(runner.commands, [])

    def test_resume_rejects_local_manifest_drift_before_ssh(self):
        output, manifest, runner = self.staged_resume()
        (output / "manifest.sha256").write_text("0" * 64 + "  manifest.json\n")
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "checksum mismatch"):
            self.publish(
                runner=runner,
                builder=lambda **_: self.fail("drifted resume rebuilt"),
                resume_staged_snapshot_id=manifest["snapshot_id"],
            )
        self.assertEqual(runner.commands, [])

    def test_resume_rejects_local_database_drift_before_ssh(self):
        output, manifest, runner = self.staged_resume()
        database = output / "databases/dcar_insight.sqlite3"
        database.write_bytes(database.read_bytes() + b"\0")
        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "dependency verification failed"
        ):
            self.publish(
                runner=runner,
                builder=lambda **_: self.fail("database-drifted resume rebuilt"),
                resume_staged_snapshot_id=manifest["snapshot_id"],
            )
        self.assertEqual(runner.commands, [])

    def test_resume_space_reserve_blocks_before_verify_or_pending(self):
        output, manifest, runner = self.staged_resume(
            free_bytes=self.config().minimum_remote_free_bytes - 1
        )
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "below the reserve"):
            self.publish(
                runner=runner,
                builder=lambda **_: self.fail("space-blocked resume rebuilt"),
                resume_staged_snapshot_id=manifest["snapshot_id"],
            )
        rendered = "\n".join(" ".join(command) for command in runner.commands)
        self.assertNotIn(" verify --bundle ", f" {rendered} ")
        self.assertNotIn(" install --bundle ", f" {rendered} ")
        self.assertFalse((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())
        self.assertFalse((output / "publisher-receipt.json").exists())

    def test_resume_verify_failure_does_not_install_or_write_pending(self):
        output, manifest, runner = self.staged_resume(fail_verify=True)
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "remote verify refused"):
            self.publish(
                runner=runner,
                builder=lambda **_: self.fail("verify-failed resume rebuilt"),
                resume_staged_snapshot_id=manifest["snapshot_id"],
            )
        rendered = "\n".join(" ".join(command) for command in runner.commands)
        self.assertIn(" verify --bundle ", f" {rendered} ")
        self.assertNotIn(" install --bundle ", f" {rendered} ")
        self.assertFalse((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())
        self.assertFalse((output / "publisher-receipt.json").exists())

    def test_resume_install_mismatch_rolls_back_and_clears_pending(self):
        _, manifest, runner = self.staged_resume(install_mismatch=True)
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "previous snapshot restored"):
            self.publish(
                runner=runner,
                builder=lambda **_: self.fail("mismatched resume rebuilt"),
                resume_staged_snapshot_id=manifest["snapshot_id"],
            )
        self.assertEqual(runner.active_database_sha256, "b" * 64)
        self.assertTrue(
            any(" rollback " in f" {' '.join(command)} " for command in runner.commands)
        )
        self.assertFalse((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())

    def test_publish_uses_strict_ssh_dry_run_space_gate_verify_then_install(self):
        runner = self.runner()
        receipt = self.publish(runner=runner)
        self.assertEqual(receipt["schema"], publisher.PUBLISHER_RECEIPT_SCHEMA)
        self.assertEqual(receipt["snapshot_id"], self.last_manifest["snapshot_id"])
        self.assertEqual(receipt["publication_status"], "partial")
        self.assertEqual(receipt["daily_report_status"], "partial")
        self.assertIsNone(receipt["weekly_report_status"])
        self.assertNotIn("capture_status", receipt)
        self.assertEqual(receipt["snapshot_contract"], descriptor())
        self.assertEqual(receipt["publication_evidence"]["preparation"]["scheduled_at"], "2026-08-28T23:30:00Z")
        self.assertEqual(receipt["publication_evidence"]["discovery"]["coverage"]["matrix_complete_windows"], 60)
        self.assertEqual(
            receipt["required_remote_bytes"],
            self.config().minimum_remote_free_bytes
            + receipt["staging_bytes"]
            + receipt["install_headroom"]["total_bytes"],
        )
        self.assertEqual(receipt["artifact_manifest_bytes"], self.last_manifest["file_byte_size"])
        self.assertEqual(receipt["optional_reuse_manifest_bytes"], self.last_manifest["optional_reuse_byte_size"])
        self.assertEqual(receipt["rsync_dry_run_transfer_bytes"], 3072)
        self.assertEqual(
            receipt["install_headroom"]["artifact_activation_bytes"],
            self.last_manifest["file_byte_size"],
        )
        self.assertEqual(receipt["install_headroom"]["artifact_backup_bytes"], 0)
        self.assertEqual(receipt["install_headroom"]["database_backup_bytes"], 4096)
        self.assertEqual(
            receipt["install_headroom"]["database_and_metadata_activation_bytes"],
            receipt["bundle_bytes"],
        )
        self.assertEqual(
            receipt["install_headroom"]["total_bytes"],
            sum(
                value
                for key, value in receipt["install_headroom"].items()
                if key != "schema" and key != "total_bytes"
            ),
        )
        self.assertEqual(receipt["remote_retention"]["retain_count"], 2)
        rendered = [" ".join(command) for command in runner.commands]
        for option in ("BatchMode=yes", "StrictHostKeyChecking=yes", "IdentitiesOnly=yes",
                       "ServerAliveInterval=30", "ServerAliveCountMax=3",
                       "--compare-dest=/var/lib/dcar-aigc/cache", "--compare-dest=/var/lib/dcar-aigc/reports"):
            self.assertTrue(any(option in command for command in rendered))
        staging = "/var/lib/dcar-aigc/incoming/" + receipt["snapshot_id"]
        for suffix in ("/artifacts/cache/", "/artifacts/reports/", "/bundle/"):
            self.assertTrue(any(staging + suffix in command for command in rendered))
        artifact_dry_runs = [
            command
            for command in runner.commands
            if "--dry-run" in command and any(
                item.startswith("--files-from=") for item in command
            )
        ]
        self.assertEqual(len(artifact_dry_runs), 2)
        for command in artifact_dry_runs:
            self.assertTrue(
                command[-1].startswith(f"dcar-prod:{staging}/artifacts/")
            )
            for option in ("--no-owner", "--no-group", "--no-perms"):
                self.assertIn(option, command)
        self.assertEqual(sum("--dry-run" in command for command in rendered), 3)
        create_incoming = next(
            index
            for index, command in enumerate(rendered)
            if " install -d " in f" {command} "
        )
        first_dry_run = next(
            index for index, command in enumerate(rendered) if "--dry-run" in command
        )
        self.assertLess(create_incoming, first_dry_run)
        positions = [next(index for index, command in enumerate(rendered) if token in f" {command} ")
                     for token in (" prune ", " verify --bundle ", " install --bundle ")]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("sudo -n", rendered[positions[1]])
        self.assertIn(f"--bundle {staging}/bundle", rendered[positions[1]])
        self.assertIn("sudo -n", rendered[positions[2]])
        self.assertFalse(any("--delete" in command for command in rendered))
        receipt_path = self.snapshot_root / "snapshot-20260829T010000Z/publisher-receipt.json"
        self.assertTrue(receipt_path.is_file())
        self.assertEqual(json.loads(receipt_path.read_bytes()), receipt)

    def test_publish_refuses_snapshot_identity_drift_before_remote_staging(self):
        def bad(**arguments):
            manifest = self.builder(**arguments)
            manifest["runtime_identity"] = {**self.fixture.runtime_identity, "matcher_rule_sha256": "e" * 64}
            return manifest
        runner = self.runner()
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "drifted from the verified writer"):
            self.publish(runner=runner, builder=bad)
        self.assertFalse(any(command[0] == "rsync" for command in runner.commands))
        self.assertFalse(any("install -d" in " ".join(command) for command in runner.commands))

    def test_publish_recomputes_evidence_from_the_frozen_snapshot(self):
        source_latest_published_at = self.freshness().latest_published_at

        def changed(**arguments):
            manifest = self.builder(**arguments)
            output = Path(arguments["output"])
            database = output / "databases/dcar_insight.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "UPDATE content_items SET published_at='2026-08-29T00:30:00Z' "
                    "WHERE id=(SELECT id FROM content_items ORDER BY id LIMIT 1)"
                )
            manifest["databases"][0].update(
                sha256=sha(database), byte_size=database.stat().st_size
            )
            self.write_manifest(output, manifest)
            return manifest

        receipt = self.publish(builder=changed)
        self.assertEqual(receipt["latest_published_at"], "2026-08-29T00:30:00Z")
        self.assertNotEqual(receipt["latest_published_at"], source_latest_published_at)

    def test_publish_refuses_invalid_snapshot_database_manifest_before_remote_staging(self):
        cases = (
            [],
            [{"name": "dcar_insight.sqlite3", "bundle_path": "../outside.sqlite3"}],
        )
        for index, databases in enumerate(cases):
            with self.subTest(databases=databases):
                def bad(**arguments):
                    manifest = self.builder(**arguments)
                    manifest["databases"] = databases
                    return manifest

                runner = self.runner()
                with self.assertRaisesRegex(
                    publisher.SnapshotPublishError,
                    "snapshot database manifest is invalid",
                ):
                    self.publish(
                        builder=bad,
                        runner=runner,
                        now=self.now + timedelta(seconds=index),
                    )
                self.assertFalse(
                    any(
                        command[0] == "rsync" or "install -d" in " ".join(command)
                        for command in runner.commands
                    )
                )

    def test_publish_refuses_snapshot_copy_that_lost_a_bound_report(self):
        def bad(**arguments):
            manifest = self.builder(**arguments)
            output = Path(arguments["output"])
            database = output / "databases/dcar_insight.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.execute("UPDATE task_events SET payload_json='{}' WHERE event_type='report_inputs_v1'")
            manifest["databases"][0].update(sha256=sha(database), byte_size=database.stat().st_size)
            self.write_manifest(output, manifest)
            return manifest
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "dependency verification failed"):
            self.publish(builder=bad)

    def test_publish_refuses_manifest_omission_of_a_real_frozen_report_file(self):
        def bad(**arguments):
            manifest = self.builder(**arguments)
            manifest["files"] = [item for item in manifest["files"] if item["root"] != "reports"]
            manifest["file_count"] = len(manifest["files"])
            manifest["file_byte_size"] = sum(item["byte_size"] for item in manifest["files"])
            self.write_manifest(Path(arguments["output"]), manifest)
            return manifest
        runner = self.runner()
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "omits a hash-bound publication evidence"):
            self.publish(builder=bad, runner=runner)
        self.assertFalse(any(command[0] == "rsync" or "install -d" in " ".join(command) for command in runner.commands))

    def _unchanged_artifact_capacity_runner(self, *, staging_headroom_bytes):
        artifact = self.project / "data/cache/v8/capacity/unchanged.json"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_bytes(b'{"payload":"' + b"x" * (8 * 1024 * 1024) + b'"}\n')
        runner = self.runner()

        def unchanged_remote(arguments, **kwargs):
            if "statvfs" in " ".join(arguments):
                output = next(self.snapshot_root.glob("snapshot-*"))
                bundle_bytes = publisher._bundle_byte_size(output)
                runner.free_bytes = (
                    self.config().minimum_remote_free_bytes
                    + 2 * bundle_bytes  # Independent staging and activation copies.
                    + runner.database_backup_bytes
                    + staging_headroom_bytes
                )
            result = runner(arguments, **kwargs)
            if arguments[0] == "rsync" and "--dry-run" in arguments:
                # No content changes or activation paths: unchanged files can
                # still be materialized as independent incoming copies.
                return subprocess.CompletedProcess(
                    arguments, 0,
                    stdout="Total transferred file size: 0 bytes\n", stderr="",
                )
            return result

        return runner, unchanged_remote

    def test_unchanged_artifacts_need_full_staging_space_before_transfer(self):
        runner, boundary = self._unchanged_artifact_capacity_runner(
            staging_headroom_bytes=4 * 1024 * 1024
        )
        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "after rsync dry-run"
        ):
            self.publish(runner=boundary)
        self.assertGreater(self.last_manifest["file_byte_size"], 8 * 1024 * 1024)
        rsyncs = [command for command in runner.commands if command[0] == "rsync"]
        self.assertEqual(len(rsyncs), 3)
        self.assertTrue(all("--dry-run" in command for command in rsyncs))
        self.assertFalse(runner.install_attempted)
        self.assertFalse(any(" verify --bundle " in " ".join(command) for command in runner.commands))
        self.assertFalse((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())
        self.assertFalse(list(self.snapshot_root.glob("snapshot-*/publisher-receipt.json")))

    def test_unchanged_artifacts_publish_with_full_staging_space_and_zero_delta(self):
        runner, boundary = self._unchanged_artifact_capacity_runner(
            staging_headroom_bytes=16 * 1024 * 1024
        )
        receipt = self.publish(runner=boundary)
        self.assertGreater(receipt["artifact_manifest_bytes"], 8 * 1024 * 1024)
        self.assertEqual(
            receipt["staging_bytes"],
            receipt["artifact_manifest_bytes"] + receipt["bundle_bytes"],
        )
        self.assertEqual(receipt["install_headroom"]["artifact_activation_bytes"], 0)
        for name in ("cache", "reports", "bundle", "transfer"):
            self.assertEqual(receipt[f"rsync_dry_run_{name}_bytes"], 0)
        self.assertEqual(sum(
            command[0] == "rsync" and "--dry-run" not in command
            for command in runner.commands
        ), 3)
        self.assertTrue(runner.install_attempted)
        self.assertEqual(runner.active_snapshot_id, receipt["snapshot_id"])

    def test_manifest_space_gate_keeps_empty_staging_but_stops_before_real_rsync(self):
        runner = self.runner(free_bytes=self.config().minimum_remote_free_bytes)
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "install headroom"):
            self.publish(runner=runner)
        rsyncs = [command for command in runner.commands if command[0] == "rsync"]
        self.assertEqual(len(rsyncs), 3)
        self.assertTrue(all("--dry-run" in command for command in rsyncs))
        rendered = [" ".join(command) for command in runner.commands]
        create_incoming = next(
            index
            for index, command in enumerate(rendered)
            if " install -d " in f" {command} "
        )
        first_dry_run = next(
            index for index, command in enumerate(rendered) if "--dry-run" in command
        )
        self.assertLess(create_incoming, first_dry_run)

    def test_rsync_transfer_space_gate_stops_before_real_transfer_or_install(self):
        runner = self.runner(dry_run_bytes=40_000_000_000)
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "after rsync dry-run"):
            self.publish(runner=runner)
        rsyncs = [command for command in runner.commands if command[0] == "rsync"]
        self.assertEqual(len(rsyncs), 3)
        self.assertTrue(all("--dry-run" in command for command in rsyncs))
        self.assertFalse(any(" verify --bundle " in " ".join(command) for command in runner.commands))
        self.assertFalse(any(" install --bundle " in " ".join(command) for command in runner.commands))

    def test_post_staging_gate_preserves_install_headroom_before_verify(self):
        reserve = self.config().minimum_remote_free_bytes
        runner = self.runner(
            free_bytes_sequence=[100_000_000_000, 100_000_000_000, reserve]
        )
        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "install headroom after staging"
        ):
            self.publish(runner=runner)
        rsyncs = [command for command in runner.commands if command[0] == "rsync"]
        self.assertEqual(sum("--dry-run" in command for command in rsyncs), 3)
        self.assertEqual(sum("--dry-run" not in command for command in rsyncs), 3)
        rendered = "\n".join(" ".join(command) for command in runner.commands)
        self.assertNotIn(" verify --bundle ", f" {rendered} ")
        self.assertNotIn(" install --bundle ", f" {rendered} ")

    def test_resume_recomputes_staged_active_install_headroom_without_transfer(self):
        reserve = self.config().minimum_remote_free_bytes
        output, manifest, runner = self.staged_resume(
            free_bytes=reserve + 2_000_000_000,
            artifact_backup_bytes=2_500_000_000,
            database_backup_bytes=500_000_000,
        )
        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "install headroom after rsync dry-run"
        ):
            self.publish(
                runner=runner,
                builder=lambda **_: self.fail("headroom resume rebuilt"),
                resume_staged_snapshot_id=manifest["snapshot_id"],
            )
        rendered = [" ".join(command) for command in runner.commands]
        self.assertEqual(sum("--dry-run" in command for command in rendered), 3)
        self.assertTrue(
            all(
                command[0] != "rsync" or "--dry-run" in command
                for command in runner.commands
            )
        )
        self.assertFalse(any(" verify --bundle " in command for command in rendered))
        self.assertFalse(any(" install --bundle " in command for command in rendered))
        self.assertFalse((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())
        self.assertFalse((output / "publisher-receipt.json").exists())

    def test_remote_install_failure_retains_local_snapshot_and_receives_no_retry_delete(self):
        runner = self.runner(fail_install=True)
        before = self.database.read_bytes()
        with self.assertRaises(publisher.SnapshotPublishError):
            self.publish(runner=runner)
        self.assertEqual(self.database.read_bytes(), before)
        self.assertTrue((self.snapshot_root / "snapshot-20260829T010000Z/databases/dcar_insight.sqlite3").is_file())
        rendered = "\n".join(" ".join(command) for command in runner.commands)
        self.assertNotIn("--delete", rendered)
        self.assertNotIn(" rm ", f" {rendered} ")

    def test_post_install_identity_mismatch_rolls_back_and_clears_pending(self):
        runner = self.runner(install_mismatch=True)
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "previous snapshot restored"):
            self.publish(runner=runner)
        self.assertEqual(runner.active_database_sha256, "b" * 64)
        self.assertTrue(any(" rollback " in f" {' '.join(command)} " for command in runner.commands))
        self.assertFalse((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())

    def test_post_install_network_failure_retains_pending_without_rollback(self):
        runner = self.create_pending()
        self.assertFalse(any(" rollback " in f" {' '.join(command)} " for command in runner.commands))
        self.assertTrue((self.snapshot_root / publisher.PENDING_STATE_FILENAME).exists())

    def test_managed_original_never_enters_files_or_optional_reuse(self):
        for target in ("files", "optional_reuse_files"):
            manifest = {"artifact_policy": publisher.ARTIFACT_POLICY, "snapshot_contract": descriptor(),
                "writer_project_root": str(self.project), "files": [], "optional_reuse_files": [],
                "managed_originals": {"contract_version": "managed-originals-v1", "bundles": [
                    {"members": [{"project_path": "data/cache/managed/original.mp4"}]}]}}
            manifest[target] = [{"project_path": "data/cache/managed/original.mp4"}]
            with self.subTest(target=target), self.assertRaisesRegex(publisher.SnapshotPublishError, "managed original"):
                publisher._validate_manifest_contract(manifest, project_root=self.project)


if __name__ == "__main__":
    unittest.main()
