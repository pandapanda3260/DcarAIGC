"""Real local bytes and rsync prove frozen publication without network access."""

from __future__ import annotations

import errno
from datetime import timedelta
import hashlib
import json
import os
import shutil
import signal
import stat
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_server_snapshot_deployment as server
from tests import test_macos_snapshot_publisher as macos

builder = server.builder
publisher = macos.publisher


class FrozenBuilderTest(unittest.TestCase):
    def setUp(self):
        self.fixture = server.ServerSnapshotDeploymentTest(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def assert_no_attempt(self):
        self.assertFalse(self.fixture.bundle.exists())
        self.assertEqual(list(self.fixture.root.glob(".bundle.*")), [])

    def test_required_files_are_independent_and_optional_media_is_not_copied(self):
        manifest = self.fixture.build_bundle()
        frozen = publisher._verify_frozen_artifacts(self.fixture.bundle, manifest)
        for row in manifest["files"]:
            original = self.fixture.project / row["project_path"]
            copy = frozen / row["root"] / row["path"]
            self.assertNotEqual(original.stat().st_ino, copy.stat().st_ino)
            self.assertEqual(copy.stat().st_nlink, 1)
            original.write_bytes(b"live source replaced after freeze")
            self.assertEqual(
                hashlib.sha256(copy.read_bytes()).hexdigest(), row["sha256"]
            )
            self.assertEqual(copy.stat().st_size, row["byte_size"])
        for row in manifest["optional_reuse_files"]:
            self.assertFalse((frozen / row["root"] / row["path"]).exists())
        publisher._verify_frozen_artifacts(self.fixture.bundle, manifest)

    def test_recursive_json_reads_captured_bytes_after_live_path_changes(self):
        register = builder._add_artifact
        changed = []

        def mutate_after_registration(*args, **kwargs):
            result = register(*args, **kwargs)
            relative = kwargs["relative_path"]
            if relative == "data/cache/v8/media/test/media.json" and not changed:
                changed.append(relative)
                (self.fixture.project / relative).write_text(
                    '{"path":"data/cache/now-missing.json"}'
                )
            return result

        with patch.object(
            builder, "_add_artifact", side_effect=mutate_after_registration
        ):
            manifest = self.fixture.build_bundle()
        self.assertEqual(len(changed), 1)
        self.assertEqual(manifest["optional_reuse_file_count"], 1)
        self.assertNotIn(
            "data/cache/now-missing.json",
            {r["project_path"] for r in manifest["files"]},
        )
        publisher._verify_frozen_artifacts(self.fixture.bundle, manifest)

    def test_real_installer_normalizes_read_only_frozen_artifacts_to_server_mode(self):
        manifest = self.fixture.build_bundle()
        frozen = publisher._verify_frozen_artifacts(self.fixture.bundle, manifest)
        config = self.fixture.server_config()
        config.database_root.mkdir(parents=True)
        server._create_old_active_database(
            config.database_root / "dcar_insight.sqlite3"
        )
        shutil.copytree(frozen, self.fixture.bundle.parent / "artifacts")
        actions = []
        receipt = server.installer.install_bundle(
            self.fixture.bundle,
            config,
            service_action=actions.append,
            smoke_check=lambda: None,
        )
        self.assertEqual(receipt["snapshot_id"], manifest["snapshot_id"])
        self.assertEqual(actions, ["stop", "start"])
        for row in manifest["files"]:
            root = config.cache_root if row["root"] == "cache" else config.reports_root
            installed = root / row["path"]
            self.assertEqual(stat.S_IMODE(installed.stat().st_mode), 0o640)
            self.assertEqual(
                hashlib.sha256(installed.read_bytes()).hexdigest(), row["sha256"]
            )

    def test_in_place_mutation_during_copy_rejects_and_cleans_only_attempt(self):
        original_read = os.read
        target = self.fixture.project / "data/cache/.comment_hash_salt"
        identity = target.stat().st_ino
        mutated = []

        def race(fd, size):
            block = original_read(fd, size)
            if block and os.fstat(fd).st_ino == identity and not mutated:
                mutated.append(True)
                target.write_bytes(b"changed during the copy")
            return block

        with (
            patch.object(builder.os, "read", side_effect=race),
            self.assertRaisesRegex(
                builder.SnapshotBuildError, "changed while freezing"
            ),
        ):
            self.fixture.build_bundle()
        self.assertTrue(mutated)
        self.assert_no_attempt()
        self.assertTrue(target.is_file())

    def test_enospc_and_keyboard_interrupt_remove_partial_private_tree(self):
        for failure in (
            OSError(errno.ENOSPC, "fixture disk full"),
            KeyboardInterrupt(),
        ):
            with self.subTest(failure=type(failure).__name__):
                with (
                    patch.object(builder, "_write_from0_lists", side_effect=failure),
                    self.assertRaises(type(failure)),
                ):
                    self.fixture.build_bundle()
                self.assert_no_attempt()
                self.assertTrue(self.fixture.database.is_file())

    def test_sigterm_cleans_private_tree_and_restores_callers_handler(self):
        previous = signal.getsignal(signal.SIGTERM)

        def terminate(*_args):
            os.kill(os.getpid(), signal.SIGTERM)

        with (
            patch.object(builder, "_write_from0_lists", side_effect=terminate),
            self.assertRaises(SystemExit),
        ):
            self.fixture.build_bundle()
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)
        self.assert_no_attempt()

    def test_disk_reserve_blocks_freezing_without_publishing(self):
        with (
            patch.object(
                builder.shutil, "disk_usage", return_value=SimpleNamespace(free=0)
            ),
            self.assertRaisesRegex(
                builder.SnapshotBuildError, "insufficient local space"
            ),
        ):
            self.fixture.build_bundle()
        self.assert_no_attempt()

    def test_frozen_symlink_hardlink_tamper_and_path_escape_are_rejected(self):
        manifest = self.fixture.build_bundle()
        row = manifest["files"][0]
        path = (
            self.fixture.bundle
            / publisher.FROZEN_ARTIFACT_DIRECTORY
            / row["root"]
            / row["path"]
        )
        original = path.read_bytes()
        live = self.fixture.project / row["project_path"]
        for kind in ("symlink", "hardlink", "tamper"):
            with self.subTest(kind=kind):
                path.unlink()
                if kind == "symlink":
                    path.symlink_to(live)
                elif kind == "hardlink":
                    os.link(live, path)
                else:
                    path.write_bytes(b"corrupt")
                    path.chmod(0o400)
                with self.assertRaises(publisher.SnapshotPublishError):
                    publisher._verify_frozen_artifacts(self.fixture.bundle, manifest)
                path.unlink()
                path.write_bytes(original)
                path.chmod(0o400)
        row["path"] = "../escape"
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "path is invalid"):
            publisher._verify_frozen_artifacts(self.fixture.bundle, manifest)

    def test_file_list_must_match_manifest_exactly(self):
        manifest = self.fixture.build_bundle()
        with (self.fixture.bundle / "cache-files-from0").open("ab") as handle:
            handle.write(b"unlisted\0")
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "list differs"):
            publisher._verify_frozen_artifacts(self.fixture.bundle, manifest)

    def test_repeated_reference_keeps_strict_registered_identity_types(self):
        relative = "data/cache/one-byte"
        (self.fixture.project / relative).write_bytes(b"1")
        files = builder._FrozenArtifacts(self.fixture.root / "frozen")
        arguments = dict(project_root=self.fixture.project, relative_path=relative)
        builder._add_artifact(files, [], **arguments)
        for expected in (
            {"expected_byte_size": True},
            {"expected_byte_size": 1.0},
            {"expected_sha256": 1},
        ):
            with (
                self.subTest(expected=expected),
                self.assertRaises(builder.SnapshotBuildError),
            ):
                builder._add_artifact(files, [], **arguments, **expected)
        builder._add_artifact(
            files,
            [],
            **arguments,
            expected_byte_size=1,
            expected_sha256=hashlib.sha256(b"1").hexdigest(),
        )


class FrozenPublisherTest(unittest.TestCase):
    def setUp(self):
        self.fixture = macos.MacOSSnapshotPublisherTest(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.live = self.fixture.project / "data/cache/v8/duplicates/mutable.json"
        self.live.parent.mkdir(parents=True, exist_ok=True)
        self.live.write_text('{"generation":"captured"}')
        self.captured = self.live.read_bytes()
        self.remote = self.fixture.project.parent / "offline-remote"

    def real_local_rsync(self, remote_runner):
        def run(arguments, **kwargs):
            if arguments[0] == "rsync" and "--dry-run" not in arguments:
                destination = arguments[-1]
                suffix = (
                    "bundle"
                    if destination.endswith("/bundle/")
                    else "artifacts/"
                    + ("cache" if destination.endswith("/cache/") else "reports")
                )
                target = self.remote / suffix
                target.mkdir(parents=True, exist_ok=True)
                args = []
                skip = False
                for arg in arguments[:-2]:
                    if skip:
                        skip = False
                        continue
                    if arg == "-e":
                        skip = True
                        continue
                    if arg.startswith(("--compare-dest=", "--copy-dest=")):
                        continue
                    args.append(arg)
                result = subprocess.run(
                    [*args, arguments[-2], str(target) + "/"],
                    text=True,
                    capture_output=True,
                    timeout=30,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
            return remote_runner(arguments, **kwargs)

        return run

    def test_live_mutation_after_build_sends_original_bytes_without_duplicate_bundle(
        self,
    ):
        build = self.fixture.builder

        def mutate(**kwargs):
            manifest = build(**kwargs)
            self.live.write_text('{"generation":"new live bytes"}')
            return manifest

        runner = self.fixture.runner()
        self.fixture.publish(builder=mutate, runner=self.real_local_rsync(runner))
        self.assertEqual(
            (self.remote / "artifacts/cache/v8/duplicates/mutable.json").read_bytes(),
            self.captured,
        )
        self.assertFalse((self.remote / "bundle/frozen-artifacts").exists())
        manifest = json.loads((self.remote / "bundle/manifest.json").read_text())
        for row in manifest["files"]:
            path = self.remote / "artifacts" / row["root"] / row["path"]
            self.assertEqual(
                hashlib.sha256(path.read_bytes()).hexdigest(), row["sha256"]
            )
        output = next(self.fixture.snapshot_root.glob("snapshot-*"))
        required_bytes = manifest["file_byte_size"]
        all_bytes = sum(p.stat().st_size for p in output.rglob("*") if p.is_file())
        self.assertEqual(
            publisher._bundle_byte_size(output), all_bytes - required_bytes
        )

    def test_local_resume_uses_frozen_bytes_and_missing_copies_never_fallback(self):
        output, manifest, runner = self.fixture.staged_resume()
        self.live.write_text('{"generation":"new live bytes"}')
        with patch.object(publisher.Path, "home", return_value=self.fixture.fake_home):
            publisher.publish_snapshot(
                project_root=self.fixture.project,
                database=None,
                legacy_database=None,
                config=self.fixture.config(),
                now=self.fixture.now,
                runner=self.real_local_rsync(runner),
                fetch_json=lambda _: self.fail("local resume queried Writer"),
                resume_local_snapshot_id=manifest["snapshot_id"],
            )
        self.assertEqual(
            (self.remote / "artifacts/cache/v8/duplicates/mutable.json").read_bytes(),
            self.captured,
        )
        shutil.rmtree(output / publisher.FROZEN_ARTIFACT_DIRECTORY)
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher._verify_frozen_artifacts(output, manifest)

    def test_old_staged_only_resume_remains_supported_without_live_artifact_send(self):
        build = self.fixture.builder

        def old_snapshot(**kwargs):
            manifest = build(**kwargs)
            manifest.pop("local_artifact_source")
            self.fixture.write_manifest(kwargs["output"], manifest)
            shutil.rmtree(kwargs["output"] / publisher.FROZEN_ARTIFACT_DIRECTORY)
            return manifest

        with patch.object(self.fixture, "builder", side_effect=old_snapshot):
            _, manifest, runner = self.fixture.staged_resume()
        self.fixture.publish(
            runner=runner,
            resume_staged_snapshot_id=manifest["snapshot_id"],
            builder=lambda **_: self.fail("staged resume rebuilt"),
        )
        self.assertFalse(
            any(
                args[0] == "rsync" and "--dry-run" not in args
                for args in runner.commands
            )
        )

    def old_staged_snapshot(self):
        build = self.fixture.builder

        def old_snapshot(**kwargs):
            manifest = build(**kwargs)
            manifest.pop("local_artifact_source")
            self.fixture.write_manifest(kwargs["output"], manifest)
            shutil.rmtree(kwargs["output"] / publisher.FROZEN_ARTIFACT_DIRECTORY)
            return manifest

        with patch.object(self.fixture, "builder", side_effect=old_snapshot):
            return self.fixture.staged_resume()

    def test_old_local_resume_without_frozen_bytes_stops_before_ssh(self):
        _, manifest, runner = self.old_staged_snapshot()
        with (
            patch.object(publisher.Path, "home", return_value=self.fixture.fake_home),
            self.assertRaisesRegex(
                publisher.SnapshotPublishError, "no frozen artifact source"
            ),
        ):
            publisher.publish_snapshot(
                project_root=self.fixture.project,
                database=None,
                legacy_database=None,
                config=self.fixture.config(),
                now=self.fixture.now,
                runner=runner,
                fetch_json=lambda _: self.fail("local resume queried Writer"),
                resume_local_snapshot_id=manifest["snapshot_id"],
            )
        self.assertEqual(runner.commands, [])

    def test_old_remote_drift_failure_next_automatic_attempt_builds_new_frozen_bytes(
        self,
    ):
        old_output, old_manifest, runner = self.old_staged_snapshot()
        old_files = {
            path.relative_to(old_output): path.read_bytes()
            for path in old_output.rglob("*")
            if path.is_file()
        }

        # This is the exact pre-install remote boundary of the production
        # failure. An old staged bundle needs no nonexistent local frozen files.
        def drift_at_verify(arguments, **kwargs):
            if " verify --bundle " in " " + " ".join(arguments) + " ":
                return subprocess.CompletedProcess(
                    arguments,
                    1,
                    stdout="",
                    stderr=(
                        "snapshot operation refused: artifact drifted: "
                        "/incoming/artifacts/cache/v8/duplicates/mutable.json"
                    ),
                )
            return runner(arguments, **kwargs)

        with self.assertRaisesRegex(
            publisher.SnapshotPublishError, "artifact drifted"
        ) as failure:
            self.fixture.publish(
                runner=drift_at_verify,
                resume_staged_snapshot_id=old_manifest["snapshot_id"],
            )
        publisher._record_publisher_status(
            self.fixture.config(),
            {"status": "blocked", "reason": str(failure.exception)},
        )
        self.assertIsNone(publisher._read_pending_state(self.fixture.snapshot_root))
        self.assertFalse(runner.install_attempted)
        self.live.write_text('{"generation":"current after old failure"}')
        current_bytes = self.live.read_bytes()
        built = []

        def new_snapshot(**arguments):
            manifest = self.fixture.builder(**arguments)
            manifest["snapshot_id"] = (
                "20260829T020000Z-" + manifest["databases"][0]["sha256"][:12]
            )
            self.fixture.write_manifest(arguments["output"], manifest)
            built.append(arguments["output"])
            return manifest

        receipt = self.fixture.publish(
            automatic=True,
            now=self.fixture.now + timedelta(hours=1),
            builder=new_snapshot,
            runner=self.real_local_rsync(runner),
        )
        self.assertEqual(len(built), 1)
        self.assertNotEqual(receipt["snapshot_id"], old_manifest["snapshot_id"])
        self.assertEqual(
            (self.remote / "artifacts/cache/v8/duplicates/mutable.json").read_bytes(),
            current_bytes,
        )
        self.assertEqual(
            {
                path.relative_to(old_output): path.read_bytes()
                for path in old_output.rglob("*")
                if path.is_file()
            },
            old_files,
        )
        self.assertFalse((old_output / publisher.FROZEN_ARTIFACT_DIRECTORY).exists())
        self.assertFalse((old_output / "publisher-receipt.json").exists())
        self.assertTrue((built[0] / "publisher-receipt.json").is_file())


if __name__ == "__main__":
    unittest.main()
