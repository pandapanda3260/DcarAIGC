"""Exercise the real isolated guard on temporary source; never SSH or use services."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shlex
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "dcar_receiver_binding_publisher", ROOT / "deploy/macos/publish_snapshot.py"
)
assert SPEC is not None and SPEC.loader is not None
publisher = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = publisher
SPEC.loader.exec_module(publisher)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class SnapshotReceiverBindingTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="receiver-binding-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / "release/source"
        self.entry = self.source / publisher.RECEIVER_ENTRYPOINT
        self.entry.parent.mkdir(parents=True)
        self.helper = self.source / "src/helper.py"
        self.helper.parent.mkdir()
        self.helper.write_text('VALUE = "verified dependency"\n')
        self.marker = self.root / "executed.json"
        self.entry.write_text(
            'import json, sys\nfrom pathlib import Path\n'
            'sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))\n'
            'import helper\n'
            f'Path({str(self.marker)!r}).write_text(json.dumps(sys.argv))\n'
            'SUPPORTED_SCHEMA_VERSIONS = {19, 20, 21}\n'
            'SUPPORTED_SCHEMA_TRANSITIONS = {(19, 20), (20, 21)}\n'
            'if __name__ == "__main__":\n'
            '    operation = sys.argv[1]\n'
            '    print(json.dumps({"status": "verified", "snapshot_id": "fixture",\n'
            '        "schema": "dcar-read-replica-" + operation + "-receipt-v1",\n'
            '        "retain_count": 2, "argv": sys.argv, "helper": helper.VALUE}))\n'
        )
        self.manifest_path = self.source.parent / "source-manifest.json"
        self.manifest = {
            "schema": "dcar-snapshot-receiver-source-v1",
            "files": [dict(path=p.relative_to(self.source).as_posix(), sha256=sha(p), size=p.stat().st_size)
                      for p in (self.entry, self.helper)],
        }
        self.binding = {
            "schema": "dcar-snapshot-receiver-binding-v1",
            "source_root": str(self.source),
            "manifest_path": str(self.manifest_path),
            "manifest_sha256": "",
            "entrypoint_sha256": sha(self.entry),
        }
        self.seal_manifest()
        self.config = publisher.PublishConfig(
            ssh_alias="unused", remote_project_root="/var/www/dcar-aigc/current",
            remote_state_root="/var/lib/dcar-aigc", remote_python=sys.executable,
            snapshot_root=self.root / "snapshots", minimum_remote_free_bytes=1 << 30,
            expected_user_version=21, maximum_content_lag_days=1,
        )
        self.commands = []
        self.binding_patch = patch.object(publisher, "_receiver_binding", side_effect=lambda: dict(self.binding))
        self.binding_patch.start()
        self.addCleanup(self.binding_patch.stop)

    def seal_manifest(self):
        self.manifest_path.write_text(json.dumps(self.manifest))
        self.binding["manifest_sha256"] = sha(self.manifest_path)

    def runner(self, arguments, **kwargs):
        command = shlex.split(arguments[-1])
        self.commands.append(command)
        if command[:2] == ["sudo", "-n"]:
            command = command[2:]
        # Run the actual guard and probe import, excluding service/HTTP probes.
        if publisher.REMOTE_PROBE_SCHEMA in command[4]:
            command[4] = command[4].split("\ndef fetch(url):", 1)[0] + (
                '\nprint(json.dumps({"current_release": str(project_root),'
                '"installer_schema_support": installer_schema_support}))'
            )
        return subprocess.run(command, **kwargs)

    def call(self, operation="verify"):
        return publisher._remote_installer_operation(
            self.config, ["unused-ssh"], operation, runner=self.runner,
            bundle="/fixture bundle/$literal", snapshot_id="fixture snapshot",
        )

    def assert_refused(self):
        with self.assertRaises(publisher.SnapshotPublishError):
            self.call()
        self.assertFalse(self.marker.exists(), "entrypoint executed before verification")

    def test_cli_preserves_arguments_for_all_operations(self):
        for operation in ("verify", "install", "rollback"):
            with self.subTest(operation=operation):
                value = self.call(operation)
                self.assertEqual(value["argv"], [str(self.entry), operation, "--bundle",
                    "/fixture bundle/$literal", "--snapshot-id", "fixture snapshot", "--expected-schema", "21"])
                self.assertEqual(value["helper"], "verified dependency")
                self.assertEqual(self.config.remote_project_root, "/var/www/dcar-aigc/current")
        self.assertFalse(list(self.source.rglob("__pycache__")))

    def test_help_and_prune_share_verified_execution(self):
        publisher._check_remote_sudo(self.config, ["unused-ssh"], runner=self.runner)
        self.assertEqual(json.loads(self.marker.read_text()), [str(self.entry), "--help"])
        publisher._prune_remote_snapshots(self.config, ["unused-ssh"], runner=self.runner)
        self.assertEqual(json.loads(self.marker.read_text()), [str(self.entry), "prune",
            "--incoming-root", "/var/lib/dcar-aigc/incoming", "--retain-count", "2"])
        self.assertTrue(all(c[:5] == ["sudo", "-n", sys.executable, "-I", "-B"] for c in self.commands))

    def test_probe_import_is_verified_and_preserves_current(self):
        value = publisher._remote_probe(self.config, ["unused-ssh"], runner=self.runner)
        self.assertEqual(value["current_release"], self.config.remote_project_root)
        self.assertEqual(value["installer_schema_support"]["versions"], [19, 20, 21])
        self.assertTrue(self.marker.exists())
        self.marker.unlink()
        self.helper.write_text('VALUE = "tampered dependency"\n')
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher._remote_probe(self.config, ["unused-ssh"], runner=self.runner)
        self.assertFalse(self.marker.exists())

    def test_changed_dependency_never_executes(self):
        self.helper.write_text('VALUE = "tampered dependency"\n')
        self.assert_refused()

    def test_changed_manifest_never_executes(self):
        self.manifest_path.write_text(self.manifest_path.read_text() + " ")
        self.assert_refused()

    def test_changed_entrypoint_never_executes(self):
        self.entry.write_text(self.entry.read_text() + "# changed\n")
        self.assert_refused()

    def test_independent_entrypoint_pin_is_required(self):
        self.binding["entrypoint_sha256"] = "a" * 64
        self.assert_refused()

    def test_missing_dependency_never_executes(self):
        self.helper.unlink()
        self.assert_refused()

    def test_missing_manifest_never_executes(self):
        self.manifest_path.unlink()
        self.assert_refused()

    def test_entrypoint_must_be_listed(self):
        self.manifest["files"] = self.manifest["files"][1:]
        self.seal_manifest()
        self.assert_refused()

    def test_wrong_size_never_executes(self):
        self.manifest["files"][1]["size"] += 1
        self.seal_manifest()
        self.assert_refused()

    def test_path_escape_is_rejected(self):
        self.manifest["files"][1]["path"] = "../outside.py"
        self.seal_manifest()
        self.assert_refused()

    def test_symlink_dependency_is_rejected_even_when_bytes_match(self):
        outside = self.root / "helper.py"
        self.helper.replace(outside)
        self.helper.symlink_to(outside)
        self.assert_refused()

    def test_symlink_directory_is_rejected(self):
        outside = self.root / "src"
        self.helper.parent.replace(outside)
        self.helper.parent.symlink_to(outside, target_is_directory=True)
        self.assert_refused()

    def test_unlisted_python_and_bytecode_are_rejected(self):
        for name in ("injected.py", "helper.pyc"):
            with self.subTest(name=name):
                extra = self.source / "src" / name
                extra.write_text("unverified")
                self.assert_refused()
                extra.unlink()

    def test_unlisted_symlink_is_rejected(self):
        (self.source / "extra").symlink_to(self.root, target_is_directory=True)
        self.assert_refused()

    def test_duplicate_manifest_entry_is_rejected(self):
        self.manifest["files"].append(dict(self.manifest["files"][1]))
        self.seal_manifest()
        self.assert_refused()

    def test_unfrozen_or_missing_local_binding_never_sends_a_command(self):
        self.binding_patch.stop()
        binding_path = self.root / "snapshot_receiver.json"
        runner = Mock(side_effect=AssertionError("SSH must not be called"))
        with patch.object(publisher, "RECEIVER_BINDING_PATH", binding_path):
            for value in (None, {**self.binding, "manifest_sha256": "UNFROZEN"}):
                if value is not None:
                    binding_path.write_text(json.dumps(value))
                with self.assertRaises(publisher.SnapshotPublishError):
                    publisher._check_remote_sudo(self.config, ["unused-ssh"], runner=runner)
        runner.assert_not_called()

    def test_frozen_binding_selects_receiver_without_changing_current(self):
        self.binding_patch.stop()
        binding_path = self.root / "snapshot_receiver.json"
        binding_path.write_text(json.dumps(self.binding))
        with patch.object(publisher, "RECEIVER_BINDING_PATH", binding_path), patch.object(
            publisher, "RECEIVER_SOURCE_ROOT", str(self.source)
        ):
            self.assertEqual(publisher._receiver_binding(), self.binding)
            self.assertEqual(self.config.remote_installer, str(self.entry))
            self.assertEqual(self.config.remote_project_root, "/var/www/dcar-aigc/current")
            value = self.call()
            self.assertEqual(value["argv"][0], str(self.entry))

    def test_untrusted_pythonpath_is_ignored_before_guard_runs(self):
        inject = self.root / "injected"
        inject.mkdir()
        injection_marker = self.root / "injection-ran"
        (inject / "sitecustomize.py").write_text(
            f'from pathlib import Path; Path({str(injection_marker)!r}).touch()\n'
        )
        with patch.dict(os.environ, {"PYTHONPATH": str(inject)}):
            self.call()
        self.assertFalse(injection_marker.exists())


if __name__ == "__main__":
    unittest.main()
