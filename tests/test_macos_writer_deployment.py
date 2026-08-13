from __future__ import annotations

import os
import plistlib
import re
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MACOS_DEPLOY = ROOT / "deploy" / "macos"


class MacOSWriterDeploymentTest(unittest.TestCase):
    def test_launch_agent_is_disabled_and_uses_separate_worker_port(self) -> None:
        template = (MACOS_DEPLOY / "cn.tj.dcar.writer-worker.plist.template").read_text(
            encoding="utf-8"
        )
        rendered = template.replace("__PROJECT_ROOT_XML__", "/tmp/DcarAIGC")
        rendered = rendered.replace("__HOME_XML__", "/tmp/dcar-home")
        value = plistlib.loads(rendered.encode("utf-8"))
        environment = value["EnvironmentVariables"]

        self.assertEqual(value["Label"], "cn.tj.dcar.writer-worker")
        self.assertTrue(value["Disabled"])
        self.assertTrue(value["RunAtLoad"])
        self.assertTrue(value["KeepAlive"])
        self.assertEqual(environment["DCAR_READ_ONLY"], "0")
        self.assertEqual(environment["DCAR_SCHEDULER_ENABLED"], "1")
        self.assertEqual(environment["DCAR_STARTUP_CATCHUP_ENABLED"], "0")
        self.assertEqual(environment["DCAR_WORKER_HOST"], "127.0.0.1")
        self.assertEqual(environment["DCAR_WORKER_PORT"], "8766")
        self.assertEqual(
            environment["DCAR_WRITER_LOCK"],
            "/tmp/DcarAIGC/runtime/writer-worker.lock",
        )
        self.assertIn("/opt/homebrew/bin", environment["PATH"])
        self.assertIn("/usr/bin", environment["PATH"])
        self.assertNotIn("8765", template)
        self.assertFalse(any("API_KEY" in key for key in environment))

    def test_renderer_validates_in_memory_without_installing(self) -> None:
        path = MACOS_DEPLOY / "render_launch_agent.py"
        result = subprocess.run(
            [
                sys.executable,
                str(path),
                "--project-root",
                str(ROOT),
                "--home",
                "/tmp/dcar-home",
                "--check",
            ],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self.assertIn("valid disabled LaunchAgent", result.stdout)

    def test_wrapper_requires_external_key_file_and_cost_authorization(self) -> None:
        wrapper = (MACOS_DEPLOY / "run_writer_worker.sh").read_text(encoding="utf-8")
        example = (MACOS_DEPLOY / "writer.env.example").read_text(encoding="utf-8")
        readme = (MACOS_DEPLOY / "README.md").read_text(encoding="utf-8")

        self.assertIn("I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_8", wrapper)
        self.assertIn("TIKHUB_API_KEY_FILE", wrapper)
        self.assertIn("caffeinate -s", wrapper)
        self.assertIn("--host 127.0.0.1", wrapper)
        self.assertIn("--port 8766", wrapper)
        self.assertIn("ffmpeg ffprobe swiftc", wrapper)
        self.assertIn("import mlx_whisper", wrapper)
        self.assertNotRegex(example, re.compile(r"^TIKHUB_API_KEY\s*=", re.MULTILINE))
        self.assertRegex(
            example,
            re.compile(r"^DCAR_DAILY_COST_AUTHORIZATION=$", re.MULTILINE),
        )
        self.assertNotIn("I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_8", example)
        self.assertIn("USD 8", readme)
        self.assertIn("must stay powered, connected to the network, and awake", readme)
        self.assertIn("never calls `launchctl`", readme)

    def test_first_start_bootstraps_once_then_waits_for_run_at_load(self) -> None:
        readme = (MACOS_DEPLOY / "README.md").read_text(encoding="utf-8")
        first_start = readme.split(
            "## 3. Explicitly enable and wait for first start", maxsplit=1
        )[1].split("### Deliberate restart", maxsplit=1)[0]
        deliberate_restart = readme.split(
            "### Deliberate restart of an already loaded worker", maxsplit=1
        )[1].split("## Stop, disable, and uninstall", maxsplit=1)[0]

        self.assertIn('launchctl enable "$domain/$label"', first_start)
        self.assertIn('launchctl bootstrap "$domain" "$plist"', first_start)
        self.assertNotIn("launchctl kickstart", first_start)
        self.assertIn("for attempt in $(seq 1 60)", first_start)
        self.assertIn("http://127.0.0.1:8766/api/v8/health", first_start)
        self.assertIn('launchctl kickstart -k "$domain/$label"', deliberate_restart)


if __name__ == "__main__":
    unittest.main()
