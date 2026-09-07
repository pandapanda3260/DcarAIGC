"""Offline file/SQLite tests for the paired 18-to-19 server transition.

The old release is an explicit contract fixture: current source with the three
historical version literals, not evidence of a deployed or restarted v18 app.
Only OS service/HTTP boundaries are substituted; verification and recovery run
against actual release files, frozen v18 DDL and a real v19 snapshot bundle.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests import test_server_snapshot_deployment as deployment
from tests.schema_fixture import initialize_historical_schema
from v8 import media_consumer_proofs, storage


installer = deployment.installer
SOURCE_ROOT = Path(__file__).resolve().parents[1]


def _write(path: Path, payload: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(mode)


def _source_template(root: Path) -> None:
    """Copy the real code closures without dependencies or any user data."""
    trees = {
        "src", "config", "deploy/server", "deploy/macos", "scripts",
        *media_consumer_proofs._CODE_DIRS,
    }
    sources: set[Path] = set()
    for tree in trees:
        directory = SOURCE_ROOT / tree
        if directory.is_dir():
            sources.update(
                path for path in directory.rglob("*")
                if path.is_file() and "__pycache__" not in path.parts
                and path.suffix not in {".pyc", ".pyo"}
            )
    sources.update(SOURCE_ROOT / name for name in media_consumer_proofs._CODE_FILES)
    sources.add(SOURCE_ROOT / "deploy/server/requirements-api.txt")
    for source in sources:
        target = root / source.relative_to(SOURCE_ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        target.chmod(0o755 if source.stat().st_mode & 0o111 else 0o644)
    # A static response fixture keeps the inventory nonempty without building
    # or claiming to exercise the Web application in this engine-only suite.
    _write(root / "app/web/dist/index.html", b"<!doctype html><p>fixture</p>\n", 0o644)
    _write(
        root / "app/web/node_modules/vinext/dist/cli.js",
        b"#!/usr/bin/env node\n",
        0o755,
    )
    _write(root / ".venv/pyvenv.cfg", b"include-system-site-packages = false\n", 0o644)
    executable = root / ".venv/bin/python"
    executable.parent.mkdir(parents=True)
    shutil.copy2(Path(sys.executable).resolve(), executable)
    executable.chmod(0o755)


def _historical_database(path: Path, version: int = 18) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        storage.configure_connection_safety(connection)
        initialize_historical_schema(
            connection, target_version=17 if version == 18 else version
        )
        if version == 18:
            storage.migrate_database(
                connection, from_version=17, to_version=18
            )
        connection.executescript(
            """
            INSERT INTO taxonomy_versions(id,version,status,definition,created_at)
            VALUES('old-taxonomy','selling-points-v5.2','published','{}',
                   '2026-08-28T00:00:00Z');
            INSERT INTO evaluation_releases(
                id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                created_at,updated_at
            ) VALUES(
                'evaluation-v9__selling-points-v5.2','evaluation-v9',
                'selling-points-v5.2',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                'active','2026-08-28T00:00:00Z','2026-08-28T00:00:00Z'
            );
            INSERT INTO content_items(
                id,link_id,platform,canonical_url,imported_at,created_at,updated_at
            ) VALUES(99,'OLD999','douyin','https://www.douyin.com/video/99',
                     '2026-08-28T00:00:00Z','2026-08-28T00:00:00Z',
                     '2026-08-28T00:00:00Z');
            """
        )
    path.chmod(0o640)


class SimulatedPowerLoss(BaseException):
    """Bypasses the Exception handler without killing the test interpreter."""


class ServerSchemaUpgradeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.template_directory = tempfile.TemporaryDirectory()
        cls.template = Path(cls.template_directory.name).resolve() / "code"
        _source_template(cls.template)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.template_directory.cleanup()

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.writer = self.root / "writer"
        self.writer.mkdir()
        database = self.writer / "app/data/dcar_insight.sqlite3"
        database.parent.mkdir(parents=True)
        deployment._create_main_database(database, self.writer)
        legacy = database.with_name("web_mvp.sqlite3")
        deployment._create_legacy_database(legacy)
        self.bundle = self.root / "incoming/bundle"
        self.manifest = deployment.builder.build_snapshot(
            project_root=self.writer, database=database, legacy_database=legacy,
            output=self.bundle, expected_user_version=19,
        )
        for item in self.manifest["files"]:
            target = self.bundle.parent / "artifacts" / item["root"] / item["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.writer / item["project_path"], target)

        server = self.root / "server"
        self.config = installer.InstallConfig(
            database_root=server / "db", cache_root=server / "cache",
            reports_root=server / "reports", runtime_root=server / "runtime",
            current_release=server / "current", releases_root=server / "releases",
            systemd_root=server / "systemd", nginx_root=server / "nginx",
            config_root=server / "configuration",
        )
        self.old = self.config.releases_root / "schema18"
        self.new = self.config.releases_root / "schema19"
        for release in (self.old, self.new):
            shutil.copytree(self.template, release)
        old_storage = self.old / "src/dcar_eval/v8/storage.py"
        source = old_storage.read_text(encoding="utf-8")
        self.assertIn("SCHEMA_VERSION = 19", source)
        source = source.replace("SCHEMA_VERSION = 19", "SCHEMA_VERSION = 18", 1)
        source = source.replace(
            'CURRENT_SCHEMA_MIGRATION_NAME = "dual-acquisition-profile-roster-v1"',
            'CURRENT_SCHEMA_MIGRATION_NAME = "matrix-roster-source-routing"', 1,
        )
        old_storage.write_text(source, encoding="utf-8")
        contracts = self.old / "src/dcar_eval/v8/contracts.py"
        contracts.write_text(
            contracts.read_text(encoding="utf-8").replace(
                'CURRENT_REPORT_VERSION = "dcar-content-operations-report-v8.9"',
                'CURRENT_REPORT_VERSION = "dcar-content-operations-report-v8.8"', 1,
            ), encoding="utf-8",
        )
        for relative in (
            "deploy/server/install_snapshot.py",
            "deploy/macos/publish_snapshot.py",
            "scripts/build_server_snapshot.py",
        ):
            contract = self.old / relative
            payload = contract.read_text(encoding="utf-8")
            payload = payload.replace(
                'EXPECTED_REPORT_VERSION = "dcar-content-operations-report-v8.9"',
                'EXPECTED_REPORT_VERSION = "dcar-content-operations-report-v8.8"',
                1,
            )
            payload = payload.replace(
                "EXPECTED_DATABASE_SCHEMA_VERSION = 19",
                "EXPECTED_DATABASE_SCHEMA_VERSION = 18",
                1,
            )
            payload = payload.replace(
                'EXPECTED_DATABASE_SCHEMA_MIGRATION = "dual-acquisition-profile-roster-v1"',
                'EXPECTED_DATABASE_SCHEMA_MIGRATION = "matrix-roster-source-routing"',
                1,
            )
            contract.write_text(payload, encoding="utf-8")
        self.config.current_release.symlink_to(self.old, target_is_directory=True)
        self.active_database = self.config.database_root / "dcar_insight.sqlite3"
        _historical_database(self.active_database)
        old_legacy = self.config.database_root / "web_mvp.sqlite3"
        deployment._create_legacy_database(old_legacy)
        with sqlite3.connect(old_legacy) as connection:
            connection.execute("UPDATE legacy_marker SET value='old legacy'")
        old_legacy.chmod(0o640)

        # Cover changed evidence, unchanged salts, an absent new report, and
        # unrelated old media that the transition must leave untouched.
        for item in self.manifest["files"]:
            if item["root"] != "cache":
                continue
            payload = (self.writer / item["project_path"]).read_bytes()
            if item["path"].endswith("media.json"):
                payload = b'{"old_evidence": true}\n'
            _write(self.config.cache_root / item["path"], payload, 0o640)
        _write(self.config.cache_root / "unlisted/keep.bin", b"unrelated historical media", 0o640)
        self.config.reports_root.mkdir(parents=True)
        _write(self.config.active_manifest_path, b'{"snapshot_id":"20260828T010000Z-000000000028","old":true}\n')
        for key, target in installer._config_targets(self.config).items():
            target.parent.mkdir(parents=True, exist_ok=True)
            if key != "nginx/dcar-proxy.conf":
                _write(target, ("# old fixture: " + key + "\n").encode(), 0o600)
        # macOS /private/tmp inherits wheel, even for a non-wheel user. Real
        # server-managed files use the configured owner/group; align fixtures
        # before sealing so fchown recovery is real and needs no privilege mock.
        for path in (self.root, *self.root.rglob("*")):
            if not path.is_symlink():
                os.chown(path, os.getuid(), os.getgid())
        self.events: list[tuple[str, str]] = []
        self.services = {
            service: {"LoadState": "loaded", "ActiveState": "active",
                      "SubState": "running", "MainPID": str(100 + index)}
            for index, service in enumerate(installer.SCHEMA_SERVICES)
        }
        self.service_patch = patch.object(installer, "_service_states", side_effect=self._service_states)
        self.service_patch.start()
        self.addCleanup(self.service_patch.stop)
        self.baseline = self._protected_state()

    def _service_states(self) -> dict[str, dict[str, str]]:
        return {service: dict(state) for service, state in self.services.items()}

    def _service_action(self, verb: str, service: str) -> None:
        self.assertIn(service, installer.SCHEMA_SERVICES)
        self.assertIn(verb, {"stop", "start"})
        self.events.append((verb, service))
        state = self.services[service]
        state.update(
            ActiveState="active" if verb == "start" else "inactive",
            SubState="running" if verb == "start" else "dead",
            MainPID="200" if verb == "start" else "0",
        )

    def _reload_configuration(self) -> None:
        self.events.append(("reload", "configuration"))

    def _protected_state(self) -> dict[str, object]:
        records: dict[str, object] = {"current": str(self.config.current_release.resolve())}
        roots = (self.config.database_root, self.config.cache_root, self.config.reports_root,
                 self.config.systemd_root, self.config.nginx_root, self.config.config_root)
        for root in roots:
            for path in root.rglob("*"):
                if path.is_file():
                    records[str(path.relative_to(self.root))] = (
                        deployment._sha256(path), path.stat().st_size,
                        stat.S_IMODE(path.stat().st_mode), path.stat().st_uid, path.stat().st_gid,
                    )
        receipt = self.config.active_manifest_path
        records["active_receipt"] = (
            receipt.read_bytes(), stat.S_IMODE(receipt.stat().st_mode),
            receipt.stat().st_uid, receipt.stat().st_gid,
        )
        return records

    def _assert_old_pair(self) -> None:
        self.assertEqual(self._protected_state(), self.baseline)
        with sqlite3.connect(f"{self.active_database.as_uri()}?mode=ro", uri=True) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 18)
            self.assertEqual(connection.execute("SELECT id FROM content_items").fetchall(), [(99,)])
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
        self.events.append(("smoke", "old_pair"))

    def _assert_new_pair(self) -> dict[str, object]:
        self.assertEqual(self.config.current_release.resolve(), self.new)
        with sqlite3.connect(f"{self.active_database.as_uri()}?mode=ro", uri=True) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 19)
            self.assertEqual(connection.execute("SELECT id FROM content_items").fetchall(), [(1,)])
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
        for item in self.manifest["databases"]:
            self.assertEqual(deployment._sha256(self.config.database_root / item["name"]), item["sha256"])
        for item in self.manifest["files"]:
            root = self.config.cache_root if item["root"] == "cache" else self.config.reports_root
            path = root / item["path"]
            staged = self.bundle.parent / "artifacts" / item["root"] / item["path"]
            self.assertEqual(deployment._sha256(path), item["sha256"])
            self.assertEqual(deployment._sha256(staged), item["sha256"])
            self.assertEqual(path.stat().st_nlink, 1)
            self.assertNotEqual(path.stat().st_ino, staged.stat().st_ino)
        for key, target in installer._config_targets(self.config).items():
            expected = (("# old fixture: " + key + "\n").encode() if key.startswith("config/")
                        else (self.new / "deploy/server" / key).read_bytes())
            self.assertEqual(target.read_bytes(), expected)
        receipt = json.loads(self.config.active_manifest_path.read_bytes())
        self.assertEqual(receipt["snapshot_id"], self.manifest["snapshot_id"])
        self.assertEqual(receipt["runtime_identity"], self.manifest["runtime_identity"])
        self.assertEqual(receipt["snapshot_contract"], self.manifest["snapshot_contract"])
        self.assertEqual((self.config.cache_root / "unlisted/keep.bin").read_bytes(),
                         b"unrelated historical media")
        self.events.append(("smoke", "new_pair"))
        return {"fixture_only": True, "database_user_version": 19, "files_verified": len(self.manifest["files"])}

    def _seal(self) -> dict[str, object]:
        return installer.seal_schema_upgrade(
            self.bundle, self.config, release_dir=self.new, expected_current_release=self.old,
            from_schema=18, to_schema=19,
        )

    def _replace_bundle_and_release(self) -> tuple[Path, dict[str, object]]:
        database = self.writer / "app/data/dcar_insight.sqlite3"
        with sqlite3.connect(database) as connection:
            connection.execute(
                "UPDATE content_items SET updated_at='2026-09-02T00:00:00Z' WHERE id=1"
            )
        bundle = self.root / "replacement-incoming/bundle"
        manifest = deployment.builder.build_snapshot(
            project_root=self.writer,
            database=database,
            legacy_database=database.with_name("web_mvp.sqlite3"),
            output=bundle,
            expected_user_version=19,
        )
        for item in manifest["files"]:
            target = bundle.parent / "artifacts" / item["root"] / item["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.writer / item["project_path"], target)
        replacement = self.config.releases_root / "schema19-replacement"
        shutil.copytree(self.template, replacement)
        self.bundle = bundle
        self.manifest = manifest
        self.new = replacement
        return bundle, manifest

    def _upgrade(
        self, *, from_schema: int = 18, to_schema: int = 19, **kwargs: object
    ) -> dict[str, object]:
        return installer.schema_upgrade(
            self.bundle, self.config, release_dir=self.new, expected_current_release=self.old,
            service_action=self._service_action, reload_configuration=self._reload_configuration,
            smoke_check=self._assert_new_pair, rollback_smoke_check=self._assert_old_pair,
            from_schema=from_schema, to_schema=to_schema,
            **kwargs,
        )

    def _rolled_back_first_attempt(
        self, *, checkpoint: str = "smoke_verified"
    ) -> dict[str, object]:
        self._seal()

        def fail_first_attempt(name: str) -> None:
            if name == checkpoint:
                raise RuntimeError("first sealed release cannot start")

        with self.assertRaisesRegex(
            installer.SnapshotInstallError, "sealed old.*restored"
        ):
            self._upgrade(checkpoint_hook=fail_first_attempt)
        prior = json.loads(self.config.transition_path.read_bytes())
        self.assertEqual(prior["status"], "rolled_back")
        self.assertEqual(prior["attempt"], 1)
        return prior

    def test_sealed_upgrade_is_idempotent_and_ordinary_rollback_cannot_cross_schema(self) -> None:
        seal = self._seal()
        self.assertEqual(seal["old_runtime_identity"]["database_schema_version"], 18)
        self.assertEqual(seal["runtime_identity"]["database_schema_version"], 19)
        self.assertEqual(len(seal["runtime_identity"]), 10)
        self.assertEqual(self._seal(), seal)
        self.assertEqual(self._protected_state(), self.baseline)
        self.assertEqual(self.events, [])
        result = self._upgrade()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["attempt"], 1)
        self.assertIsNotNone(result["completed_at"])
        self.assertEqual(result["code_sha256"], media_consumer_proofs.code_sha256(self.new))
        self.assertEqual(result, json.loads(self.config.transition_path.read_bytes()))
        self.assertEqual([event for event in self.events if event[0] == "stop"],
                         [("stop", service) for service in installer.SCHEMA_SERVICES])
        self.assertEqual([event for event in self.events if event[0] == "start"],
                         [("start", service) for service in reversed(installer.SCHEMA_SERVICES)])
        after = self._protected_state()
        events = list(self.events)
        self.assertEqual(self._upgrade(), result)
        self.assertEqual(self._protected_state(), after)
        self.assertEqual(self.events, events)

        # Ordinary history is deliberately populated with a real v18 backup;
        # refusal must be the schema guard, not a missing history-file error.
        old_history_id = "20260828T010000Z-000000000028"
        old_history = self.config.history_root / old_history_id
        old_history.mkdir()
        backup = Path(result["backup_dir"]) / "databases/dcar_insight.sqlite3"
        shutil.copy2(backup, old_history / "dcar_insight.sqlite3")
        with self.assertRaisesRegex(installer.SnapshotInstallError, "database_schema_version"):
            installer.rollback_snapshot(
                self.config, snapshot_id=old_history_id,
                service_action=lambda verb: self.events.append((verb, "ordinary")),
                smoke_check=lambda: self.fail("cross-schema rollback reached smoke"),
            )
        self.assertEqual(self._protected_state(), after)
        self.assertEqual(self.events, events)

    def test_checkpoint_exceptions_restore_the_entire_old_pair(self) -> None:
        seal = self._seal()
        checkpoints = ("backup_complete", "configurations_applied", "artifacts_applied",
                       "databases_applied", "receipt_activated", "code_activated", "smoke_verified")
        for attempt, checkpoint in enumerate(checkpoints, 1):
            with self.subTest(checkpoint=checkpoint):
                visited: list[str] = []

                def fail_at(name: str) -> None:
                    visited.append(name)
                    if name == checkpoint:
                        raise RuntimeError("injected:" + checkpoint)

                with self.assertRaisesRegex(installer.SnapshotInstallError, "sealed old.*restored") as raised:
                    self._upgrade(checkpoint_hook=fail_at)
                self.assertIsInstance(raised.exception.__cause__, RuntimeError)
                self.assertEqual(str(raised.exception.__cause__), "injected:" + checkpoint)
                self.assertIn(checkpoint, visited)
                transition = json.loads(self.config.transition_path.read_bytes())
                self.assertEqual(transition["status"], "rolled_back")
                self.assertEqual(transition["attempt"], attempt)
                self.assertEqual(transition["checkpoint"], checkpoint)
                self.assertIsNotNone(transition["completed_at"])
                self._assert_old_pair()
                self.assertEqual(installer._code_inventory(self.old), seal["old_code"])
                self.assertEqual(installer._code_inventory(self.new), seal["new_code"])
                self.assertTrue(all(state["ActiveState"] == "active" for state in self.services.values()))

    def test_power_loss_leaves_in_progress_then_recovers_before_retry(self) -> None:
        self._seal()

        def power_loss(name: str) -> None:
            if name == "code_activated":
                raise SimulatedPowerLoss("simulated abrupt stop")

        with self.assertRaises(SimulatedPowerLoss):
            self._upgrade(checkpoint_hook=power_loss)
        interrupted = json.loads(self.config.transition_path.read_bytes())
        self.assertEqual(interrupted["status"], "in_progress")
        self.assertEqual(interrupted["checkpoint"], "code_activated")
        self.assertIsNone(interrupted["completed_at"])
        self.assertEqual(self.config.current_release.resolve(), self.new)
        self.assertTrue(all(state["ActiveState"] == "inactive" for state in self.services.values()))
        with self.assertRaisesRegex(installer.SnapshotInstallError, "unsettled schema transition"):
            installer.install_bundle(
                self.bundle, self.config,
                service_action=lambda verb: self.fail("ordinary install controlled a service"),
                smoke_check=lambda: self.fail("ordinary install reached smoke"),
            )
        self.events.clear()
        result = self._upgrade()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["attempt"], 2)
        self.assertLess(self.events.index(("smoke", "old_pair")), self.events.index(("smoke", "new_pair")))
        self.assertTrue((Path(interrupted["backup_dir"]) / "sealed-manifest.json").is_file())
        self.assertNotEqual(result["backup_dir"], interrupted["backup_dir"])
        self._assert_new_pair()

    def test_succeeded_17_to18_transition_is_preserved_in_18_to19_receipt(self) -> None:
        predecessor = {
            "schema": installer.LEGACY_SCHEMA_TRANSITION_CONTRACT,
            "status": "in_progress",
            "from_schema": 17,
            "to_schema": 18,
            "completed_at": None,
            "snapshot_id": "20260829T021304Z-0ddeec3ccc47",
        }
        _write(
            self.config.transition_path,
            (json.dumps(predecessor) + "\n").encode(),
        )
        with self.assertRaisesRegex(
            installer.SnapshotInstallError, "unsettled schema transition"
        ):
            self._seal()
        predecessor.update(
            status="succeeded", completed_at="2026-08-29T02:17:50Z"
        )
        _write(
            self.config.transition_path,
            (json.dumps(predecessor) + "\n").encode(),
        )
        self._seal()
        result = self._upgrade()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["predecessor_transition"], predecessor)
        self.assertEqual(
            json.loads(self.config.transition_path.read_bytes())["predecessor_transition"],
            predecessor,
        )
        self._assert_new_pair()

    def test_rolled_back_attempt_can_use_a_fresh_snapshot_and_release(self) -> None:
        first = self._rolled_back_first_attempt()

        first_snapshot = first["snapshot_id"]
        first_manifest_sha = first["snapshot_manifest_sha256"]
        _, replacement_manifest = self._replace_bundle_and_release()
        self.assertNotEqual(replacement_manifest["snapshot_id"], first_snapshot)
        self.assertNotEqual(
            deployment._sha256(self.bundle / "manifest.json"), first_manifest_sha
        )
        self._seal()
        original_file_record = installer._file_record
        first_backup = Path(first["backup_dir"])

        def backup_with_distinct_group(path: Path, **kwargs: object) -> dict[str, object]:
            record = original_file_record(path, **kwargs)
            if first_backup in path.parents:
                record = {**record, "gid": int(record["gid"]) + 1}
            return record

        with patch.object(
            installer, "_file_record", side_effect=backup_with_distinct_group
        ):
            result = self._upgrade()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["attempt"], 2)
        self.assertEqual(result["superseded_transition"], first)
        self.assertEqual(result["snapshot_id"], replacement_manifest["snapshot_id"])
        self.assertEqual(
            Path(result["backup_dir"]),
            self.config.history_root
            / replacement_manifest["snapshot_id"]
            / "schema-upgrade/attempt-2",
        )
        self.assertEqual(self.config.current_release.resolve(), self.new)
        self._assert_new_pair()

    def test_fresh_transition_accepts_rollback_before_backup(self) -> None:
        first = self._rolled_back_first_attempt(checkpoint="prepared")
        self.assertFalse((Path(first["backup_dir"]) / "sealed-manifest.json").exists())
        _, replacement_manifest = self._replace_bundle_and_release()
        self._seal()
        result = self._upgrade()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["attempt"], 2)
        self.assertEqual(result["snapshot_id"], replacement_manifest["snapshot_id"])
        self.assertEqual(result["superseded_transition"], first)

    def test_fresh_transition_rejects_non_rolled_back_prior_states(self) -> None:
        prior = self._rolled_back_first_attempt()
        self._replace_bundle_and_release()
        self._seal()
        before = self._protected_state()
        events = list(self.events)
        mutations = (
            (
                "in_progress",
                {"status": "in_progress", "completed_at": None},
                "unfinished or previous transition",
            ),
            (
                "rollback_failed",
                {"status": "rollback_failed", "completed_at": None},
                "unfinished or previous transition",
            ),
            ("succeeded", {"status": "succeeded"}, "unfinished or previous transition"),
            (
                "different_pair",
                {
                    "schema": installer.LEGACY_SCHEMA_TRANSITION_CONTRACT,
                    "from_schema": 17,
                    "to_schema": 18,
                },
                "unfinished or previous transition",
            ),
            (
                "different_old_release",
                {"old_release": str(self.new)},
                "unfinished or previous transition",
            ),
            ("missing_completion", {"completed_at": None}, "unfinished or previous transition"),
            ("invalid_attempt", {"attempt": 0}, "unfinished or previous transition"),
            (
                "invalid_completion",
                {"completed_at": "not-a-time"},
                "completion time is invalid",
            ),
        )
        for label, update, message in mutations:
            with self.subTest(label=label):
                changed = {**prior, **update}
                _write(
                    self.config.transition_path,
                    (json.dumps(changed) + "\n").encode(),
                )
                with self.assertRaisesRegex(
                    installer.SnapshotInstallError,
                    message,
                ):
                    self._upgrade()
                self.assertEqual(self._protected_state(), before)
                self.assertEqual(self.events, events)
        _write(
            self.config.transition_path,
            (json.dumps(prior) + "\n").encode(),
        )

    def test_fresh_transition_rejects_incomplete_or_tampered_rollback(self) -> None:
        first = self._rolled_back_first_attempt()
        self._replace_bundle_and_release()
        self._seal()
        before = self._protected_state()
        saved_seal = Path(first["backup_dir"]) / "sealed-manifest.json"
        sealed = json.loads(saved_seal.read_bytes())
        sealed["snapshot_id"] = "20260902T000000Z-000000000000"
        _write(
            saved_seal,
            (json.dumps(sealed) + "\n").encode(),
        )
        with self.assertRaisesRegex(
            installer.SnapshotInstallError, "rollback backup seal changed"
        ):
            self._upgrade()
        self.assertEqual(self._protected_state(), before)
        self.assertEqual(
            json.loads(self.config.transition_path.read_bytes()), first
        )

    def test_source17_and_non_18_to19_requests_are_refused_before_service_changes(self) -> None:
        original = self.active_database.read_bytes()
        source17 = self.root / "source17.sqlite3"
        _historical_database(source17, version=17)
        shutil.copy2(source17, self.active_database)
        before = self._protected_state()
        with self.assertRaisesRegex(installer.SnapshotInstallError, "SQLite schema mismatch.*17, expected 18"):
            self._seal()
        self.assertEqual(self._protected_state(), before)
        self.assertEqual(self.events, [])
        self.assertFalse((self.new / "schema-upgrade-manifest.json").exists())
        _write(self.active_database, original, 0o640)
        self._seal()
        with self.assertRaisesRegex(installer.SnapshotInstallError, "only sealed 17-to-18, 18-to-19 or 19-to-20"):
            self._upgrade(from_schema=17, to_schema=19)
        self.assertEqual(self._protected_state(), self.baseline)
        self.assertEqual(self.events, [])

    def test_seal_rejects_wrong_pair_and_nonexact_schema19_structure(self) -> None:
        with self.assertRaisesRegex(
            installer.SnapshotInstallError,
            "only sealed 17-to-18, 18-to-19 or 19-to-20",
        ):
            installer.seal_schema_upgrade(
                self.bundle,
                self.config,
                release_dir=self.new,
                expected_current_release=self.old,
                from_schema=18,
                to_schema=20,
            )
        self.assertFalse((self.new / "schema-upgrade-manifest.json").exists())

        malformed = self.root / "incoming/malformed"
        shutil.copytree(self.bundle, malformed)
        manifest_path = malformed / "manifest.json"
        manifest = json.loads(manifest_path.read_bytes())
        main = next(
            row
            for row in manifest["databases"]
            if row["name"] == "dcar_insight.sqlite3"
        )
        database = malformed / main["bundle_path"]
        with sqlite3.connect(database) as connection:
            connection.execute("DROP INDEX idx_profile_activations_effective")
        main["byte_size"] = database.stat().st_size
        main["sha256"] = deployment._sha256(database)
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (malformed / "manifest.sha256").write_text(
            deployment._sha256(manifest_path) + "  manifest.json\n",
            encoding="ascii",
        )
        with self.assertRaisesRegex(
            installer.SnapshotInstallError,
            "exact sealed schema structure",
        ):
            installer.seal_schema_upgrade(
                malformed,
                self.config,
                release_dir=self.new,
                expected_current_release=self.old,
                from_schema=18,
                to_schema=19,
            )
        self.assertFalse((self.new / "schema-upgrade-manifest.json").exists())
        self.assertEqual(self._protected_state(), self.baseline)
        self.assertEqual(self.events, [])

    def test_supported_contract_sets_are_exact_and_keep_17_to18(self) -> None:
        self.assertEqual(installer.SUPPORTED_SCHEMA_VERSIONS, {17, 18, 19, 20})
        self.assertEqual(
            installer.SUPPORTED_SCHEMA_TRANSITIONS,
            {(17, 18), (18, 19), (19, 20)},
        )
        self.assertEqual(
            installer._transition_contract(17, 18),
            (
                installer.LEGACY_SCHEMA_TRANSITION_CONTRACT,
                installer.LEGACY_SCHEMA_SEAL_CONTRACT,
            ),
        )
        installer._verify_release_contract(self.old, 18)
        compatibility_installer = (
            self.old / "deploy/server/install_snapshot.py"
        ).read_text(encoding="utf-8")
        self.assertIn(
            '19: (\n        "dcar-content-operations-report-v8.9",\n'
            '        "dual-acquisition-profile-roster-v1",\n    )',
            compatibility_installer,
        )
        script = """
import importlib.util
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location("compat_installer", sys.argv[1])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
config = module.InstallConfig(
    database_root=Path(sys.argv[3]),
    cache_root=Path(sys.argv[4]),
    reports_root=Path(sys.argv[5]),
    runtime_root=Path(sys.argv[6]),
    current_release=Path(sys.argv[7]),
    releases_root=Path(sys.argv[8]),
    systemd_root=Path(sys.argv[9]),
    nginx_root=Path(sys.argv[10]),
    config_root=Path(sys.argv[11]),
)
manifest = module.verify_bundle(
    Path(sys.argv[2]), config, expected_schema=19
)
assert manifest["runtime_identity"]["database_schema_version"] == 19
module._service_states = lambda: {
    service: {
        "LoadState": "loaded",
        "ActiveState": "active",
        "SubState": "running",
        "MainPID": str(300 + index),
    }
    for index, service in enumerate(module.SCHEMA_SERVICES)
}
seal = module.seal_schema_upgrade(
    Path(sys.argv[2]),
    config,
    release_dir=Path(sys.argv[12]),
    expected_current_release=Path(sys.argv[13]),
    from_schema=18,
    to_schema=19,
)
assert seal["schema"] == "dcar-schema18-to19-release-seal-v1"
"""
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
                str(self.old / "deploy/server/install_snapshot.py"),
                str(self.bundle),
                str(self.config.database_root),
                str(self.config.cache_root),
                str(self.config.reports_root),
                str(self.config.runtime_root),
                str(self.config.current_release),
                str(self.config.releases_root),
                str(self.config.systemd_root),
                str(self.config.nginx_root),
                str(self.config.config_root),
                str(self.new),
                str(self.old),
            ],
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONPATH": ""},
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        with self.assertRaisesRegex(
            installer.SnapshotInstallError,
            "sealed release has incompatible",
        ):
            installer._verify_release_contract(self.old, 20)
        with self.assertRaisesRegex(
            installer.SnapshotInstallError,
            "schema21 has no supported release contract",
        ):
            installer._verify_release_contract(self.old, 21)

    def test_changed_seal_and_release_code_fail_closed(self) -> None:
        self._seal()
        seal_path = self.new / "schema-upgrade-manifest.json"
        checksum_path = self.new / "schema-upgrade-manifest.sha256"
        original_seal, original_checksum = seal_path.read_bytes(), checksum_path.read_bytes()
        changed = json.loads(original_seal)
        changed["schema"] = "unsupported-transition-contract"
        _write(seal_path, (json.dumps(changed) + "\n").encode())
        _write(checksum_path, (deployment._sha256(seal_path) + "  " + seal_path.name + "\n").encode())
        with self.assertRaisesRegex(installer.SnapshotInstallError, "unsupported schema-upgrade seal"):
            self._upgrade()
        _write(seal_path, original_seal)
        _write(checksum_path, original_checksum)
        source = self.new / "src/dcar_eval/v8/contracts.py"
        source.write_bytes(source.read_bytes() + b"\n# changed after seal\n")
        with self.assertRaisesRegex(installer.SnapshotInstallError, "sealed release files changed"):
            self._upgrade()
        self.assertEqual(self._protected_state(), self.baseline)
        self.assertEqual(self.events, [])
        self.assertFalse(self.config.transition_path.exists())

    def test_upgrade_seal_install_and_rollback_share_the_same_nonblocking_lock(self) -> None:
        self._seal()
        operations = (
            self._seal, self._upgrade,
            lambda: installer.install_bundle(self.bundle, self.config),
            lambda: installer.rollback_snapshot(self.config),
        )
        with installer._install_lock(self.config):
            for operation in operations:
                with self.subTest(operation=operation):
                    with self.assertRaisesRegex(installer.SnapshotInstallError, "snapshot install lock is busy"):
                        operation()
        self.assertEqual(self._protected_state(), self.baseline)
        self.assertEqual(self.events, [])
        self.assertFalse(self.config.transition_path.exists())


if __name__ == "__main__":
    unittest.main()
