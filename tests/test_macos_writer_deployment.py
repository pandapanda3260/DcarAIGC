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
RECONCILE_FROM = "2026-08-21"


class MacOSWriterDeploymentTest(unittest.TestCase):
    def test_launch_agent_is_disabled_and_uses_separate_worker_port(self) -> None:
        template = (MACOS_DEPLOY / "cn.tj.dcar.writer-worker.plist.template").read_text(
            encoding="utf-8"
        )
        rendered = template.replace("__PROJECT_ROOT_XML__", "/tmp/DcarAIGC")
        rendered = rendered.replace("__HOME_XML__", "/tmp/dcar-home")
        rendered = rendered.replace("__RECONCILE_FROM_XML__", RECONCILE_FROM)
        value = plistlib.loads(rendered.encode("utf-8"))
        environment = value["EnvironmentVariables"]

        self.assertEqual(value["Label"], "cn.tj.dcar.writer-worker")
        self.assertTrue(value["Disabled"])
        self.assertTrue(value["RunAtLoad"])
        self.assertTrue(value["KeepAlive"])
        self.assertEqual(environment["DCAR_READ_ONLY"], "0")
        self.assertEqual(environment["DCAR_SCHEDULER_ENABLED"], "1")
        self.assertEqual(environment["DCAR_STARTUP_CATCHUP_ENABLED"], "1")
        self.assertEqual(
            environment["DCAR_DAILY_CAPTURE_RECONCILE_FROM"], RECONCILE_FROM
        )
        self.assertEqual(environment["DCAR_WORKER_HOST"], "127.0.0.1")
        self.assertEqual(environment["DCAR_WORKER_PORT"], "8766")
        self.assertEqual(
            environment["DCAR_WRITER_LOCK"],
            "/tmp/dcar-home/Library/Application Support/DcarAIGC/runtime/"
            "writer-worker.lock",
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
                "--reconcile-from",
                RECONCILE_FROM,
                "--check",
            ],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self.assertIn("valid disabled LaunchAgent", result.stdout)

    def test_renderer_rejects_noncanonical_or_invalid_reconcile_dates(self) -> None:
        path = MACOS_DEPLOY / "render_launch_agent.py"
        for value in ("2026-8-21", "2026-02-30", " 2026-08-21", ""):
            with self.subTest(reconcile_from=value):
                result = subprocess.run(
                    [
                        sys.executable,
                        str(path),
                        "--project-root",
                        str(ROOT),
                        "--home",
                        "/tmp/dcar-home",
                        "--reconcile-from",
                        value,
                        "--check",
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("reconcile-from", result.stderr)

    def test_wrapper_requires_external_key_file_and_cost_authorization(self) -> None:
        wrapper = (MACOS_DEPLOY / "run_writer_worker.sh").read_text(encoding="utf-8")
        example = (MACOS_DEPLOY / "writer.env.example").read_text(encoding="utf-8")
        readme = (MACOS_DEPLOY / "README.md").read_text(encoding="utf-8")

        self.assertIn("I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_8", wrapper)
        self.assertIn("TIKHUB_API_KEY_FILE", wrapper)
        self.assertIn("export DCAR_STARTUP_CATCHUP_ENABLED=1", wrapper)
        self.assertIn(
            'reconcile_from="${DCAR_DAILY_CAPTURE_RECONCILE_FROM:-}"', wrapper
        )
        self.assertIn("unset DCAR_DAILY_CAPTURE_RECONCILE_FROM", wrapper)
        self.assertIn("/bin/date -j -f '%Y-%m-%d'", wrapper)
        self.assertIn(
            'export DCAR_DAILY_CAPTURE_RECONCILE_FROM="$reconcile_from"', wrapper
        )
        self.assertIn("catchup=report_only", wrapper)
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
        self.assertIn("Mac 接交流电、网络正常，且计划窗口内不睡眠", readme)
        self.assertIn("renderer 只生成 disabled-by-default plist，永不调用 `launchctl`", readme)
        self.assertIn("startup catch-up 严格为 `report_only`", readme)
        self.assertIn("不运行 capture、media 或 cutoff，不产生供应商费用", readme)
        parser = wrapper[
            wrapper.index("while IFS= read -r raw_line") : wrapper.index(
                'done < "$writer_env"'
            )
        ]
        self.assertNotIn("DCAR_DAILY_CAPTURE_RECONCILE_FROM", parser)
        self.assertGreater(
            wrapper.index(
                'export DCAR_DAILY_CAPTURE_RECONCILE_FROM="$reconcile_from"'
            ),
            wrapper.index('[[ "$cost_authorization" =='),
        )

    def test_ui_start_fails_loud_when_writer_only_flags_leak_in(self) -> None:
        script = ROOT / "scripts" / "start_web_mvp.sh"
        cases = (
            ("DCAR_SCHEDULER_ENABLED", "1"),
            ("DCAR_STARTUP_CATCHUP_ENABLED", "1"),
            ("DCAR_DAILY_CAPTURE_RECONCILE_FROM", RECONCILE_FROM),
        )
        for key, value in cases:
            with self.subTest(variable=key):
                environment = os.environ.copy()
                for variable in (
                    "DCAR_SCHEDULER_ENABLED",
                    "DCAR_STARTUP_CATCHUP_ENABLED",
                    "DCAR_DAILY_CAPTURE_RECONCILE_FROM",
                ):
                    environment.pop(variable, None)
                environment[key] = value
                result = subprocess.run(
                    ["/bin/bash", str(script)],
                    cwd=ROOT,
                    env=environment,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 78)
                self.assertIn(key, result.stderr)

    def test_writer_wrapper_validates_inherited_reconcile_date_before_env_file(
        self,
    ) -> None:
        wrapper = MACOS_DEPLOY / "run_writer_worker.sh"
        base_environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "DCAR_PROJECT_ROOT": str(ROOT),
            "DCAR_WORKER_HOST": "127.0.0.1",
            "DCAR_WORKER_PORT": "8766",
        }
        for value in (None, "2026-8-21", "2026-02-30"):
            with self.subTest(reconcile_from=value):
                environment = dict(base_environment)
                if value is not None:
                    environment["DCAR_DAILY_CAPTURE_RECONCILE_FROM"] = value
                result = subprocess.run(
                    ["/bin/bash", str(wrapper)],
                    cwd=ROOT,
                    env=environment,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 78)
                self.assertIn("DCAR_DAILY_CAPTURE_RECONCILE_FROM", result.stderr)

        accepted = subprocess.run(
            ["/bin/bash", str(wrapper)],
            cwd=ROOT,
            env={
                **base_environment,
                "DCAR_DAILY_CAPTURE_RECONCILE_FROM": RECONCILE_FROM,
            },
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(accepted.returncode, 78)
        self.assertIn("DCAR_WRITER_ENV_FILE is missing", accepted.stderr)

    def test_first_start_bootstraps_once_then_waits_for_run_at_load(self) -> None:
        readme = (MACOS_DEPLOY / "README.md").read_text(encoding="utf-8")
        first_start = readme.split("## 3. D 日启用时序", maxsplit=1)[1].split(
            "## 故意重启、更新、停用和卸载", maxsplit=1
        )[0]
        deliberate_restart = readme.split(
            "## 故意重启、更新、停用和卸载", maxsplit=1
        )[1].split("## snapshot publisher", maxsplit=1)[0]

        self.assertIn('launchctl enable "$domain/$label"', first_start)
        self.assertIn('launchctl bootstrap "$domain" "$plist"', first_start)
        self.assertNotIn("launchctl kickstart", first_start)
        self.assertIn("for attempt in $(seq 1 60)", first_start)
        self.assertIn("http://127.0.0.1:8766/api/v8/health", first_start)
        self.assertIn('launchctl kickstart -k "$domain/$label"', deliberate_restart)


if __name__ == "__main__":
    unittest.main()
