from __future__ import annotations

import json
import os
import plistlib
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MACOS_DEPLOY = ROOT / "deploy" / "macos"
RECONCILE_FROM = "2026-08-21"


class MacOSWriterDeploymentTest(unittest.TestCase):
    @staticmethod
    def _writer_health() -> dict[str, object]:
        return {
            "status": "ok",
            "mode": "local_v8",
            "read_only": False,
            "database": "dcar_insight.sqlite3",
            "database_state": {"schema_compatibility": {"compatible": True}},
        }

    @staticmethod
    def _scheduler_health() -> dict[str, object]:
        return {
            "read_only": False,
            "requested": True,
            "enabled": True,
            "writer_lock": {"held": True},
            "daily_capture_reconcile": {"enabled": True},
            "report_runtime": {"ready": True},
        }

    def _run_ui_script(
        self,
        *,
        writer_health: dict[str, object] | None = None,
        scheduler_health: dict[str, object] | None = None,
        writer_reachable: bool = True,
        viewer_state: str = "absent",
        freeze: bool = False,
        reuse: bool = False,
    ) -> tuple[subprocess.CompletedProcess[str], str]:
        script = ROOT / "scripts" / "start_web_mvp.sh"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shim_root = root / "bin"
            shim_root.mkdir()
            log = root / "invocations.log"
            freeze_lock = root / "operator-freeze.lock"
            writer_health_file = root / "writer-health.json"
            scheduler_health_file = root / "scheduler-health.json"
            viewer_health_file = root / "viewer-health.json"
            if freeze:
                freeze_lock.write_text("freeze\n", encoding="utf-8")

            writer_health_file.write_text(
                json.dumps(writer_health or self._writer_health()), encoding="utf-8"
            )
            scheduler_health_file.write_text(
                json.dumps(scheduler_health or self._scheduler_health()), encoding="utf-8"
            )
            viewer_health_file.write_text(
                json.dumps(
                    {
                        "status": "ok",
                        "mode": (
                            "local_v8" if viewer_state == "writable" else "read_only_replica"
                        ),
                        "read_only": viewer_state != "writable",
                    }
                ),
                encoding="utf-8",
            )

            (shim_root / "lsof").write_text(
                """#!/bin/bash
case "$*" in
  *-iTCP:8765*) [[ "${FAKE_VIEWER_STATE:-absent}" != "absent" ]] ;;
  *) exit 1 ;;
esac
""",
                encoding="utf-8",
            )
            (shim_root / "curl").write_text(
                """#!/bin/bash
printf 'curl %s\n' "$*" >> "$SHIM_LOG"
url="${!#}"
case " $* " in
  *" -o /dev/null "*) exit 0 ;;
esac
case "$url" in
  *127.0.0.1:8766/api/v8/health*)
    [[ "$FAKE_WRITER_REACHABLE" == "1" ]] || exit 7
    cat "$FAKE_WRITER_HEALTH_FILE"
    ;;
  *127.0.0.1:8766/api/v8/scheduler*)
    [[ "$FAKE_WRITER_REACHABLE" == "1" ]] || exit 7
    cat "$FAKE_SCHEDULER_HEALTH_FILE"
    ;;
  *127.0.0.1:8765/api/v8/health*)
    [[ "$FAKE_VIEWER_STATE" != "unknown" ]] || exit 7
    cat "$FAKE_VIEWER_HEALTH_FILE"
    ;;
  *127.0.0.1:4174/*) exit 0 ;;
  *) exit 7 ;;
esac
""",
                encoding="utf-8",
            )
            (shim_root / "npm").write_text(
                """#!/bin/bash
printf 'npm %s\n' "$*" >> "$SHIM_LOG"
exit 0
""",
                encoding="utf-8",
            )
            (shim_root / "python3").write_text(
                """#!/bin/bash
if [[ "${1:-}" == "-" ]]; then
  exec "$REAL_PYTHON" "$@"
fi
printf 'python3 %s\n' "$*" >> "$SHIM_LOG"
printf 'DCAR_AUTH_API_UPSTREAM=%s\n' "${DCAR_AUTH_API_UPSTREAM:-}" >> "$SHIM_LOG"
exit 0
""",
                encoding="utf-8",
            )
            for shim in shim_root.iterdir():
                shim.chmod(0o755)

            environment = os.environ.copy()
            for variable in (
                "DCAR_SCHEDULER_ENABLED",
                "DCAR_STARTUP_CATCHUP_ENABLED",
                "DCAR_DAILY_CAPTURE_RECONCILE_FROM",
                "DCAR_REUSE_EXISTING_READ_ONLY_API",
            ):
                environment.pop(variable, None)
            environment.update(
                {
                    "PATH": f"{shim_root}:{environment.get('PATH', '')}",
                    "REAL_PYTHON": sys.executable,
                    "SHIM_LOG": str(log),
                    "FAKE_WRITER_REACHABLE": "1" if writer_reachable else "0",
                    "FAKE_VIEWER_STATE": viewer_state,
                    "FAKE_WRITER_HEALTH_FILE": str(writer_health_file),
                    "FAKE_SCHEDULER_HEALTH_FILE": str(scheduler_health_file),
                    "FAKE_VIEWER_HEALTH_FILE": str(viewer_health_file),
                    "DCAR_OPERATOR_FREEZE_LOCK": str(freeze_lock),
                    "DCAR_AUTH_BYPASS": "1",
                }
            )
            if reuse:
                environment["DCAR_REUSE_EXISTING_READ_ONLY_API"] = "1"
            result = subprocess.run(
                ["/bin/bash", str(script)],
                cwd=ROOT,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            invocation_log = log.read_text(encoding="utf-8") if log.exists() else ""
            return result, invocation_log

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

        self.assertIn("I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_20", wrapper)
        self.assertIn("TIKHUB_API_KEY_FILE", wrapper)
        self.assertIn("TIKHUB_API_BASE=https://api.tikhub.io", wrapper)
        self.assertIn("dcar.env.local", example)
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
        self.assertNotIn("I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_20", example)
        self.assertIn("USD 20", readme)
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

    def test_ui_normal_mode_routes_to_the_healthy_writer(self) -> None:
        result, invocations = self._run_ui_script()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("本地操作台已连接正式数据库", result.stdout)
        self.assertIn("DCAR_AUTH_API_UPSTREAM=http://127.0.0.1:8766", invocations)
        self.assertIn(
            "curl -fsS -o /dev/null http://127.0.0.1:8766/api/v8/health",
            invocations,
        )
        self.assertNotIn("uvicorn v8.api:app", invocations)

        source = (ROOT / "scripts" / "start_web_mvp.sh").read_text(encoding="utf-8")
        cleanup = source.split("cleanup() {", maxsplit=1)[1].split("}", maxsplit=1)[0]
        self.assertIn("web_pid", cleanup)
        self.assertNotIn("api_pid", cleanup)
        self.assertNotIn("8766", cleanup)

    def test_ui_normal_mode_tolerates_only_a_verified_read_only_8765(self) -> None:
        accepted, invocations = self._run_ui_script(viewer_state="read_only")
        self.assertEqual(accepted.returncode, 0, accepted.stderr)
        self.assertIn("残留的 8765 只读副本", accepted.stderr)
        self.assertIn("DCAR_AUTH_API_UPSTREAM=http://127.0.0.1:8766", invocations)

        for viewer_state in ("writable", "unknown"):
            with self.subTest(viewer_state=viewer_state):
                rejected, _ = self._run_ui_script(viewer_state=viewer_state)
                self.assertEqual(rejected.returncode, 78)
                self.assertIn("8765", rejected.stderr)

    def test_ui_normal_mode_fails_closed_when_writer_is_unreachable(self) -> None:
        result, invocations = self._run_ui_script(writer_reachable=False)

        self.assertEqual(result.returncode, 78)
        self.assertIn("正式 API 127.0.0.1:8766 不可用", result.stderr)
        self.assertNotIn("dcar_auth.gateway", invocations)

    def test_ui_writer_contract_accepts_the_real_scheduler_payload_scale(self) -> None:
        scheduler = self._scheduler_health()
        scheduler["jobs"] = [{"details_json": "x" * 400_000}]

        result, invocations = self._run_ui_script(scheduler_health=scheduler)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("DCAR_AUTH_API_UPSTREAM=http://127.0.0.1:8766", invocations)

    def test_ui_normal_mode_fails_closed_when_writer_contract_is_unhealthy(self) -> None:
        cases: list[tuple[str, dict[str, object], dict[str, object]]] = []

        read_only = self._writer_health()
        read_only["read_only"] = True
        cases.append(("read_only", read_only, self._scheduler_health()))

        incompatible = self._writer_health()
        incompatible["database_state"] = {
            "schema_compatibility": {"compatible": False}
        }
        cases.append(("schema", incompatible, self._scheduler_health()))

        for key in ("requested", "enabled"):
            scheduler = self._scheduler_health()
            scheduler[key] = False
            cases.append((key, self._writer_health(), scheduler))

        writer_lock = self._scheduler_health()
        writer_lock["writer_lock"] = {"held": False}
        cases.append(("writer_lock", self._writer_health(), writer_lock))

        reconcile = self._scheduler_health()
        reconcile["daily_capture_reconcile"] = {"enabled": False}
        cases.append(("reconcile", self._writer_health(), reconcile))

        report_runtime = self._scheduler_health()
        report_runtime["report_runtime"] = {"ready": False}
        cases.append(("report_runtime", self._writer_health(), report_runtime))

        for name, writer, scheduler in cases:
            with self.subTest(case=name):
                result, invocations = self._run_ui_script(
                    writer_health=writer,
                    scheduler_health=scheduler,
                )
                self.assertEqual(result.returncode, 78)
                self.assertIn("8766 未满足", result.stderr)
                self.assertNotIn("dcar_auth.gateway", invocations)

    def test_ui_freeze_mode_requires_lock_and_verified_viewer(self) -> None:
        no_lock, _ = self._run_ui_script(reuse=True)
        self.assertEqual(no_lock.returncode, 78)
        self.assertIn("operator freeze", no_lock.stderr)

        lock_without_reuse, _ = self._run_ui_script(freeze=True)
        self.assertEqual(lock_without_reuse.returncode, 75)

        accepted, invocations = self._run_ui_script(
            freeze=True,
            reuse=True,
            viewer_state="read_only",
        )
        self.assertEqual(accepted.returncode, 0, accepted.stderr)
        self.assertIn("DCAR_AUTH_API_UPSTREAM=http://127.0.0.1:8765", invocations)
        self.assertIn(
            "curl -fsS -o /dev/null http://127.0.0.1:8765/api/v8/health",
            invocations,
        )

        rejected, _ = self._run_ui_script(
            freeze=True,
            reuse=True,
            viewer_state="writable",
        )
        self.assertEqual(rejected.returncode, 78)
        self.assertIn("不是只读副本", rejected.stderr)

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
