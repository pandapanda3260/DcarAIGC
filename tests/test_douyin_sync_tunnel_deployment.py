from __future__ import annotations

import os
import plistlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MACOS = ROOT / "deploy/macos"
SERVER_SSH = ROOT / "deploy/server/ssh"


class DouyinSyncTunnelDeploymentTest(unittest.TestCase):
    def test_renderer_produces_disabled_persistent_launch_agent(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(MACOS / "render_douyin_sync_tunnel.py"),
                "--project-root",
                str(ROOT),
                "--home",
                "/tmp/dcar-douyin-sync-home",
                "--check",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("valid disabled LaunchAgent", result.stdout)

        template = (
            MACOS / "cn.tj.dcar.douyin-sync-tunnel.plist.template"
        ).read_text(encoding="utf-8")
        rendered = template.replace("__PROJECT_ROOT_XML__", str(ROOT))
        rendered = rendered.replace("__HOME_XML__", "/tmp/dcar-douyin-sync-home")
        value = plistlib.loads(rendered.encode("utf-8"))
        self.assertEqual(value["Label"], "cn.tj.dcar.douyin-sync-tunnel")
        self.assertTrue(value["Disabled"])
        self.assertTrue(value["RunAtLoad"])
        self.assertTrue(value["KeepAlive"])
        environment = value["EnvironmentVariables"]
        self.assertEqual(
            environment["DCAR_DOUYIN_SYNC_ENV_FILE"],
            "/tmp/dcar-douyin-sync-home/Library/Application Support/"
            "DcarAIGC/douyin-sync.env",
        )
        self.assertFalse(any("MACHINE_KEY" in key for key in environment))
        self.assertNotIn(".venv", template)

    def test_tunnel_is_one_bounded_local_forward_with_keepalive(self) -> None:
        runner = (MACOS / "run_douyin_sync_tunnel.sh").read_text(encoding="utf-8")
        common = (MACOS / "douyin_sync_common.sh").read_text(encoding="utf-8")
        self.assertIn("-NT", runner)
        self.assertIn("ExitOnForwardFailure=yes", runner)
        self.assertIn("ServerAliveInterval=30", runner)
        self.assertIn("ServerAliveCountMax=3", runner)
        self.assertIn(
            '-L "127.0.0.1:${DCAR_DOUYIN_LOCAL_PORT_VALUE}:127.0.0.1:4175"',
            runner,
        )
        self.assertEqual(runner.count(" -L "), 1)
        self.assertIn('[[ "$DCAR_DOUYIN_LOCAL_PORT_VALUE" == "14175" ]]', common)
        self.assertIn("SSH alias must not declare additional forwards", common)
        self.assertIn("StrictHostKeyChecking=yes", runner)
        self.assertIn("IdentitiesOnly=yes", runner)
        self.assertNotIn("writer.env", runner)
        self.assertNotIn("publisher.env", runner)
        self.assertNotIn(".venv", runner)

    def test_sync_env_parser_is_strict_and_separate_from_writer(self) -> None:
        example = (MACOS / "douyin-sync.env.example").read_text(encoding="utf-8")
        self.assertIn("DCAR_DOUYIN_SSH_ALIAS=", example)
        self.assertIn("DCAR_DOUYIN_LOCAL_PORT=14175", example)
        self.assertIn("DCAR_DOUYIN_MACHINE_KEY_FILE=", example)
        self.assertNotIn("TIKHUB", example)
        self.assertNotIn("DCAR_DAILY_", example)
        self.assertNotIn("DCAR_DOUYIN_MACHINE_KEY=", example)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = root / "machine.key"
            key.write_text("m" * 40, encoding="utf-8")
            key.chmod(0o600)
            environment_file = root / "douyin-sync.env"
            environment_file.write_text(
                "DCAR_DOUYIN_SSH_ALIAS=dcar-douyin-sync-prod\n"
                "DCAR_DOUYIN_LOCAL_PORT=14175\n"
                f"DCAR_DOUYIN_MACHINE_KEY_FILE={key}\n",
                encoding="utf-8",
            )
            environment_file.chmod(0o600)
            load = (
                f'source "{MACOS / "douyin_sync_common.sh"}"; '
                'dcar_sync_load_env "$SYNC_ENV"; '
                'printf "%s:%s\\n" "$DCAR_DOUYIN_SSH_ALIAS_VALUE" '
                '"$DCAR_DOUYIN_LOCAL_PORT_VALUE"'
            )
            result = subprocess.run(
                ["/bin/bash", "-c", load],
                check=True,
                capture_output=True,
                text=True,
                env={**os.environ, "SYNC_ENV": str(environment_file)},
            )
            self.assertEqual(result.stdout.strip(), "dcar-douyin-sync-prod:14175")

            environment_file.write_text(
                environment_file.read_text(encoding="utf-8")
                + "DCAR_DAILY_COST_AUTHORIZATION=forbidden\n",
                encoding="utf-8",
            )
            rejected = subprocess.run(
                ["/bin/bash", "-c", load],
                check=False,
                capture_output=True,
                text=True,
                env={**os.environ, "SYNC_ENV": str(environment_file)},
            )
            self.assertEqual(rejected.returncode, 78)
            self.assertIn("unsupported sync environment entry", rejected.stderr)

    def test_identity_tilde_expands_to_home(self) -> None:
        command = (
            f'source "{MACOS / "douyin_sync_common.sh"}"; '
            'dcar_sync_expand_identity "~/.ssh/id_ed25519_dcar_douyin_sync"'
        )
        result = subprocess.run(
            ["/bin/bash", "-c", command],
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "HOME": "/tmp/dcar-sync-home"},
        )
        self.assertEqual(
            result.stdout.strip(),
            "/tmp/dcar-sync-home/.ssh/id_ed25519_dcar_douyin_sync",
        )

    def test_health_does_not_expose_machine_key_in_argv_or_logs(self) -> None:
        health = (MACOS / "check_douyin_sync_tunnel.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("/internal/v1/health", health)
        self.assertIn("curl --config -", health)
        self.assertNotIn("curl -H", health)
        self.assertNotIn("curl --header", health)
        self.assertNotIn("set -x", health)
        self.assertNotIn('echo "$machine_key"', health)
        self.assertNotIn("Machine credential:", health)

    def test_server_key_and_match_block_deny_shell_and_bound_forward(self) -> None:
        authorized = (
            SERVER_SSH / "dcar-douyin-sync.authorized_keys.example"
        ).read_text(encoding="utf-8")
        sshd = (SERVER_SSH / "60-dcar-douyin-sync.conf").read_text(
            encoding="utf-8"
        )
        self.assertIn('command="/usr/sbin/nologin"', authorized)
        self.assertIn(
            'restrict,port-forwarding,permitopen="127.0.0.1:4175"', authorized
        )
        self.assertIn("Match User dcar-douyin-sync", sshd)
        self.assertIn("AllowTcpForwarding local", sshd)
        self.assertIn("PermitOpen 127.0.0.1:4175", sshd)
        self.assertIn("ForceCommand /usr/sbin/nologin", sshd)
        for directive in (
            "PermitTTY no",
            "X11Forwarding no",
            "AllowAgentForwarding no",
            "GatewayPorts no",
            "PermitTunnel no",
            "PasswordAuthentication no",
            "KbdInteractiveAuthentication no",
        ):
            self.assertIn(directive, sshd)
        self.assertTrue(sshd.rstrip().endswith("Match all"))

    def test_start_health_stop_and_docs_define_acceptance_and_rollback(self) -> None:
        start = (MACOS / "start_douyin_sync_tunnel.sh").read_text(encoding="utf-8")
        stop = (MACOS / "stop_douyin_sync_tunnel.sh").read_text(encoding="utf-8")
        mac_readme = (MACOS / "README.md").read_text(encoding="utf-8")
        server_readme = (ROOT / "deploy/server/README.md").read_text(
            encoding="utf-8"
        )
        self.assertIn('launchctl bootstrap "$domain" "$plist"', start)
        self.assertIn("check_douyin_sync_tunnel.sh", start)
        self.assertIn('launchctl bootout "$domain" "$plist"', stop)
        self.assertIn('launchctl disable "$domain/$label"', stop)
        self.assertIn("does\nnot read `writer.env`", mac_readme)
        self.assertIn("127.0.0.1:14175:127.0.0.1:4175", mac_readme)
        self.assertIn("Machine credential", mac_readme)
        self.assertIn("dcar-douyin-sync", server_readme)
        self.assertIn("sudo sshd -t", server_readme)
        self.assertIn("normal `ssh dcar-douyin-sync-prod` shell request must fail", server_readme)
        self.assertIn(
            "forward\nto any other destination or port must fail", server_readme
        )

    def test_all_shell_scripts_parse_and_are_executable(self) -> None:
        for name in (
            "douyin_sync_common.sh",
            "run_douyin_sync_tunnel.sh",
            "check_douyin_sync_tunnel.sh",
            "start_douyin_sync_tunnel.sh",
            "stop_douyin_sync_tunnel.sh",
        ):
            with self.subTest(script=name):
                path = MACOS / name
                self.assertTrue(os.access(path, os.X_OK))
                subprocess.run(["/bin/bash", "-n", str(path)], check=True)


if __name__ == "__main__":
    unittest.main()
