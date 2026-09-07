from __future__ import annotations

import hashlib
import importlib.util
import json
import plistlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy/macos"


def load_renderer():
    spec = importlib.util.spec_from_file_location(
        "source_runtime_publisher_renderer", DEPLOY / "render_snapshot_publisher.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PublisherSourceRendererTest(unittest.TestCase):
    def test_frozen_source_preserves_data_interpreter_and_schedule(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data, source, home = (root / name for name in ("data", "source", "home"))
            source.mkdir()
            renderer = load_renderer()
            legacy = plistlib.loads(renderer.render_plist(data, home))
            frozen = plistlib.loads(renderer.render_plist(data, home, source))
            self.assertNotIn("DCAR_WRITER_SOURCE_ROOT", legacy["EnvironmentVariables"])
            self.assertEqual(frozen["WorkingDirectory"], str(data))
            self.assertEqual(
                frozen["ProgramArguments"],
                [str(source / "deploy/macos/run_snapshot_publisher.sh")],
            )
            environment = frozen["EnvironmentVariables"]
            self.assertEqual(environment["DCAR_PROJECT_ROOT"], str(data))
            self.assertEqual(environment["DCAR_WRITER_SOURCE_ROOT"], str(source))
            self.assertTrue(environment["PATH"].startswith(str(data / ".venv/bin")))
            self.assertEqual(environment["DCAR_V8_DB"], legacy["EnvironmentVariables"]["DCAR_V8_DB"])
            self.assertEqual(environment["DCAR_READ_ONLY"], "1")
            self.assertEqual(environment["DCAR_SCHEDULER_ENABLED"], "0")
            self.assertEqual(environment["DCAR_STARTUP_CATCHUP_ENABLED"], "0")
            self.assertFalse(any("API_KEY" in key for key in environment))
            for key in ("RunAtLoad", "StartInterval", "StartCalendarInterval"):
                self.assertEqual(frozen[key], legacy[key])

    def test_renderer_rejects_relative_and_symlink_source(self):
        renderer = load_renderer()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            source = root / "source"
            source.mkdir()
            link = root / "link"
            link.symlink_to(source, target_is_directory=True)
            for path in (Path("relative"), link):
                with self.subTest(path=path), self.assertRaisesRegex(ValueError, "source root"):
                    renderer.render_plist(root / "data", root / "home", path)


class PublisherLegacySourceGateTest(unittest.TestCase):
    def run_gate(self, home):
        wrapper = (DEPLOY / "run_snapshot_publisher.sh").read_text()
        start = wrapper.index("\nimport os\n", wrapper.index("<<'WRITER_SOURCE_METADATA'")) + 1
        end = wrapper.index("\nWRITER_SOURCE_METADATA", start)
        code = compile(wrapper[start:end], "publisher-source-metadata", "exec")
        with patch("pwd.getpwuid", return_value=SimpleNamespace(pw_dir=str(home))):
            exec(code, {})

    def test_missing_and_legacy_writer_keep_previous_behavior(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary).resolve()
            self.run_gate(home)
            path = home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
            path.parent.mkdir(parents=True)
            path.write_bytes(plistlib.dumps({"EnvironmentVariables": {"DCAR_PROJECT_ROOT": "/data"}}))
            path.chmod(0o644)
            before = {item.relative_to(home): item.read_bytes() for item in home.rglob("*") if item.is_file()}
            self.run_gate(home)
            self.assertEqual(before, {item.relative_to(home): item.read_bytes() for item in home.rglob("*") if item.is_file()})

    def test_legacy_publisher_rejects_installed_frozen_writer_before_import(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary).resolve()
            path = home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
            path.parent.mkdir(parents=True)
            for source in ("/frozen/source", ""):
                with self.subTest(source=source):
                    path.write_bytes(plistlib.dumps({"EnvironmentVariables": {"DCAR_WRITER_SOURCE_ROOT": source}}))
                    path.chmod(0o644)
                    with self.assertRaisesRegex(SystemExit, "explicit frozen source root"):
                        self.run_gate(home)

    def test_unsafe_or_invalid_installed_metadata_is_not_ignored(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary).resolve()
            path = home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
            path.parent.mkdir(parents=True)
            path.write_bytes(plistlib.dumps({"EnvironmentVariables": {}}))
            path.chmod(0o666)
            with self.assertRaisesRegex(SystemExit, "ownership or mode"):
                self.run_gate(home)
            path.chmod(0o644)
            path.write_bytes(plistlib.dumps({"EnvironmentVariables": []}))
            with self.assertRaisesRegex(SystemExit, "environment is invalid"):
                self.run_gate(home)


@unittest.skipUnless(sys.platform == "darwin", "LaunchAgent wrappers require macOS tools")
class FrozenSourceWrapperTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.data = self.root / "data-project"
        self.source = self.root / "frozen-source"
        self.external = self.root / "external"
        self.os_home = self.root / "os-home"
        self.events = self.root / "events.jsonl"
        for path in (
            self.data / ".venv/bin", self.data / "v8", self.source / "src/dcar_eval/v8",
            self.source / "scripts", self.source / "deploy/macos", self.external,
        ):
            path.mkdir(parents=True)
        self.runtime_python = self.data / ".venv/bin/runtime-python"
        self.runtime_python.symlink_to(sys.executable)
        # Only substitute the test OS home for the wrapper's stdlib stdin gate.
        # The unchanged gate still reads actual temporary files and hashes them.
        python = self.data / ".venv/bin/python"
        python.write_text(f"#!{sys.executable}\n" + f"""
import os, sys
runtime = {str(self.runtime_python)!r}
if sys.argv[1:4] == ['-I', '-B', '-']:
    body = sys.stdin.read()
    prelude = 'import pwd\\nfrom types import SimpleNamespace\\npwd.getpwuid = lambda uid: SimpleNamespace(pw_dir=' + repr(os.environ['TEST_OS_HOME']) + ')\\n'
    os.execv(runtime, [runtime, '-I', '-B', '-c', prelude + body, *sys.argv[4:]])
os.execv(runtime, [runtime, *sys.argv[1:]])
""")
        python.chmod(0o700)
        (self.data / "v8/__init__.py").write_text("raise RuntimeError('mutable data source imported')\n")
        (self.data / "uvicorn.py").write_text("raise RuntimeError('mutable data uvicorn imported')\n")
        (self.source / "src/dcar_eval/v8/__init__.py").write_text("")
        self.write_source("src/dcar_eval/v8/runtime_paths.py", """
import argparse, json, os, sys
from pathlib import Path
parser = argparse.ArgumentParser()
parser.add_argument('--verify-source', action='store_true')
parser.add_argument('--project-root', required=True)
parser.add_argument('--source-root', required=True)
parser.add_argument('--mode', required=True)
parser.add_argument('--build-receipt')
args = parser.parse_args()
with Path(os.environ['TEST_EVENTS']).open('a') as stream:
    stream.write(json.dumps({'event': 'verify', 'mode': args.mode,
        'project_root': args.project_root, 'source_root': args.source_root,
        'build_receipt': args.build_receipt, 'isolated': sys.flags.isolated,
        'no_bytecode': sys.dont_write_bytecode}) + '\\n')
if os.environ.get('TEST_REJECT_SOURCE') == '1':
    raise SystemExit(4)
""")
        self.write_source("src/dcar_eval/v8/runtime_database.py", self.event_script("database-check"))
        self.write_source("src/dcar_eval/mlx_whisper.py", self.event_script("dependency-import"))
        self.write_source("src/dcar_eval/uvicorn.py", self.event_script("writer-start"))
        self.write_source("deploy/macos/publish_snapshot.py", self.event_script("publisher-start"))
        self.publisher_env = self.external / "publisher.env"
        self.publisher_env.write_text("")
        self.publisher_env.chmod(0o600)
        self.receipt = self.external / "sealed-build.json"
        self.receipt.write_text('{"fixture":true}\n')
        self.receipt.chmod(0o600)
        self.seal_entrypoint()
        self.database = self.external / "business.sqlite3"
        self.legacy = self.external / "legacy.sqlite3"
        self.lock = self.external / "writer.lock"
        for path in (self.database, self.legacy, self.lock):
            path.write_bytes(b"fixture-only")
        key = self.external / "provider-config"
        key.write_text("TIKHUB_API_KEY=unit-test-provider-key\nTIKHUB_API_BASE=https://api.tikhub.dev\n")
        key.chmod(0o600)
        self.writer_env = self.external / "writer.env"
        self.writer_env.write_text(
            f"TIKHUB_API_KEY_FILE={key}\n"
            "DCAR_DAILY_COST_AUTHORIZATION=I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_100\n"
        )
        self.writer_env.chmod(0o600)
        commands = self.root / "commands"
        commands.mkdir()
        for name in ("ffmpeg", "ffprobe", "swiftc"):
            command = commands / name
            command.write_text("#!/bin/sh\nexit 0\n")
            command.chmod(0o700)
        self.environment = {
            "PATH": str(commands) + ":/usr/bin:/bin:/usr/sbin:/sbin",
            "TEST_EVENTS": str(self.events),
            "TEST_OS_HOME": str(self.os_home),
            "DCAR_PROJECT_ROOT": str(self.data),
            "DCAR_WRITER_SOURCE_ROOT": str(self.source),
            "DCAR_PUBLISHER_ENV_FILE": str(self.publisher_env),
            "DCAR_READ_ONLY": "1", "DCAR_SCHEDULER_ENABLED": "0",
            "DCAR_STARTUP_CATCHUP_ENABLED": "0", "DCAR_SCHEDULER_START_PAUSED": "1",
            "DCAR_WORKER_HOST": "127.0.0.1", "DCAR_WORKER_PORT": "8766",
            "DCAR_DAILY_CAPTURE_RECONCILE_FROM": "2026-09-07",
            "DCAR_WRITER_ENV_FILE": str(self.writer_env),
            "DCAR_LOADED_BUILD_RECEIPT": str(self.receipt),
            "DCAR_V8_DB": str(self.database), "DCAR_LEGACY_DB": str(self.legacy),
            "DCAR_WRITER_LOCK": str(self.lock),
        }

    def seal_entrypoint(self):
        def private(name, value):
            path = self.external / name
            body = (json.dumps(value, sort_keys=True) + "\n").encode()
            path.write_bytes(body)
            path.chmod(0o600)
            return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body)}
        verifier = self.source / "src/dcar_eval/v8/runtime_paths.py"
        git = {"fixture": "immutable-source"}
        manifest = private("source-tree.json", {"contract": "writer-source-tree-v1",
            "source_root": str(self.source), "git": git, "files": [{"path": "src/dcar_eval/v8/runtime_paths.py",
            "sha256": hashlib.sha256(verifier.read_bytes()).hexdigest(), "byte_size": verifier.stat().st_size,
            "mode": verifier.stat().st_mode & 0o777}]})
        plan = private("source-plan.json", {"contract": "writer-source-isolation-successor-plan-v1",
            "transition": "writer-source-isolation-20260907-v1", "source_root": str(self.source),
            "project_root": str(self.data), "git": git, "source_tree": manifest})
        payload = {"status": "succeeded", "git": git, "code_successor_plan": plan}
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        private(self.receipt.name, {"contract_version": "sealed-build-receipt-v1", "payload": payload, "payload_sha256": digest})
        plist = self.os_home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        plist.parent.mkdir(parents=True, exist_ok=True)
        plist.write_bytes(plistlib.dumps({"Label": "cn.tj.dcar.writer-worker", "WorkingDirectory": str(self.data),
            "ProgramArguments": [str(self.source / "deploy/macos/run_writer_worker.sh")],
            "EnvironmentVariables": {"DCAR_PROJECT_ROOT": str(self.data),
                "DCAR_WRITER_SOURCE_ROOT": str(self.source), "DCAR_LOADED_BUILD_RECEIPT": str(self.receipt)}}))
        plist.chmod(0o644)

    def write_source(self, name, content):
        (self.source / name).write_text(content)

    @staticmethod
    def event_script(event):
        return f"""
import json, os, sys
from pathlib import Path
with Path(os.environ['TEST_EVENTS']).open('a') as stream:
    stream.write(json.dumps({{'event': {event!r}, 'argv': sys.argv,
        'python': sys.executable, 'project_root': os.environ.get('DCAR_PROJECT_ROOT'),
        'source_root': os.environ.get('DCAR_WRITER_SOURCE_ROOT'),
        'pythonpath': os.environ.get('PYTHONPATH'), 'build_id': os.environ.get('DCAR_LOADED_BUILD_ID'),
        'read_only': os.environ.get('DCAR_READ_ONLY'),
        'scheduler': os.environ.get('DCAR_SCHEDULER_ENABLED'),
        'catchup': os.environ.get('DCAR_STARTUP_CATCHUP_ENABLED'),
        'provider_key_present': bool(os.environ.get('TIKHUB_API_KEY')),
        'safe_path': sys.flags.safe_path, 'no_bytecode': sys.dont_write_bytecode}}) + '\\n')
"""

    def run_wrapper(self, writer=False, **changes):
        wrapper = "run_writer_worker.sh" if writer else "run_snapshot_publisher.sh"
        arguments = [] if writer else ["--remote-check"]
        return subprocess.run(
            ["/bin/bash", str(DEPLOY / wrapper), *arguments], cwd=self.data,
            env={**self.environment, **changes}, text=True, capture_output=True, timeout=15,
        )

    def recorded(self):
        return [json.loads(line) for line in self.events.read_text().splitlines()] if self.events.exists() else []

    def test_publisher_verifies_before_loading_frozen_script_and_retains_data_root(self):
        result = self.run_wrapper()
        self.assertEqual(result.returncode, 0, result.stderr)
        verify, started = self.recorded()
        self.assertEqual(verify["event"], "verify")
        self.assertEqual(verify["mode"], "publisher")
        self.assertTrue(verify["isolated"] and verify["no_bytecode"])
        self.assertEqual(started["event"], "publisher-start")
        self.assertEqual(started["argv"][0], str(self.source / "deploy/macos/publish_snapshot.py"))
        self.assertEqual(started["project_root"], str(self.data))
        self.assertIn(str(self.data), started["argv"])
        self.assertNotIn("--db", started["argv"])
        self.assertEqual(started["read_only"], "1")
        self.assertEqual((started["scheduler"], started["catchup"]), ("0", "0"))
        self.assertFalse(started["provider_key_present"])
        self.assertEqual(started["python"], str(self.runtime_python))
        self.assertEqual(started["pythonpath"], f"{self.source}/src/dcar_eval:{self.source}/scripts")
        self.assertTrue(started["safe_path"] and started["no_bytecode"])
        self.assertEqual(list(self.source.rglob("__pycache__")), [])

    def test_writer_verifies_before_database_import_and_uses_frozen_app_dir(self):
        result = self.run_wrapper(writer=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        events = self.recorded()
        self.assertEqual([event["event"] for event in events], ["verify", "database-check", "dependency-import", "writer-start"])
        self.assertEqual(events[0]["build_receipt"], str(self.receipt))
        self.assertEqual(events[0]["mode"], "writer")
        started = events[-1]
        self.assertEqual(started["project_root"], str(self.data))
        self.assertEqual(started["argv"][started["argv"].index("--app-dir") + 1], str(self.source / "src/dcar_eval"))
        self.assertEqual(started["build_id"], "sha256:" + hashlib.sha256(self.receipt.read_bytes()).hexdigest())
        self.assertEqual((started["scheduler"], started["catchup"]), ("1", "0"))
        self.assertTrue(started["safe_path"] and started["no_bytecode"])
        self.assertEqual(list(self.source.rglob("__pycache__")), [])
        self.assertEqual(self.database.read_bytes(), b"fixture-only")

    def test_source_rejection_prevents_all_business_imports(self):
        for writer in (False, True):
            with self.subTest(writer=writer):
                self.events.unlink(missing_ok=True)
                result = self.run_wrapper(writer=writer, TEST_REJECT_SOURCE="1")
                self.assertEqual(result.returncode, 78, result.stderr)
                self.assertEqual([event["event"] for event in self.recorded()], ["verify"])

    def test_tampered_verifier_cannot_execute_before_its_anchor_check(self):
        verifier = self.source / "src/dcar_eval/v8/runtime_paths.py"
        verifier.write_text(self.event_script("tampered-verifier-executed"))
        for writer in (False, True):
            with self.subTest(writer=writer):
                result = self.run_wrapper(writer=writer)
                self.assertEqual(result.returncode, 78, result.stderr)
                self.assertIn("entrypoint SHA, size or mode differs", result.stderr)
                self.assertEqual(self.recorded(), [])

    def test_changed_private_plan_cannot_rebind_the_entrypoint(self):
        path = self.external / "source-plan.json"
        value = json.loads(path.read_text())
        value["source_tree"]["sha256"] = "f" * 64
        path.write_text(json.dumps(value))
        for writer in (False, True):
            with self.subTest(writer=writer):
                result = self.run_wrapper(writer=writer)
                self.assertEqual(result.returncode, 78)
                self.assertIn("private reference SHA or size differs", result.stderr)
                self.assertEqual(self.recorded(), [])

    def test_publisher_rejects_credentials_before_source_verification(self):
        for key in ("TIKHUB_API_KEY", "TIKHUB_API_KEY_FILE"):
            with self.subTest(key=key):
                result = self.run_wrapper(**{key: "unit-test-value"})
                self.assertEqual(result.returncode, 78)
                self.assertIn("must not receive a provider key", result.stderr)
                self.assertEqual(self.recorded(), [])

    def test_empty_relative_and_symlink_source_fail_closed(self):
        link = self.root / "source-link"
        link.symlink_to(self.source, target_is_directory=True)
        for source in ("", "relative", str(link)):
            for writer in (False, True):
                with self.subTest(source=source, writer=writer):
                    result = self.run_wrapper(writer=writer, DCAR_WRITER_SOURCE_ROOT=source)
                    self.assertEqual(result.returncode, 78)
                    self.assertIn("writer source root", result.stderr)
                    self.assertEqual(self.recorded(), [])


if __name__ == "__main__":
    unittest.main()
