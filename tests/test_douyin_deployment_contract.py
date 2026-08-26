from __future__ import annotations

import os
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DouyinDeploymentContractTestCase(unittest.TestCase):
    def test_control_unit_is_loopback_only_hardened_and_fail_closed(self) -> None:
        unit = (
            ROOT / "deploy/server/systemd/dcar-douyin-control.service"
        ).read_text(encoding="utf-8")
        self.assertIn("User=dcar-douyin", unit)
        self.assertIn("Group=dcar-douyin", unit)
        self.assertIn(
            "ConditionPathIsDirectory=/var/lib/dcar-aigc/douyin-control", unit
        )
        self.assertRegex(unit, r"After=.*dcar-douyin-egress\.service")
        self.assertRegex(unit, r"Wants=.*dcar-douyin-egress\.service")
        self.assertIn("--host 127.0.0.1 --port 4175 --workers 1 --no-access-log", unit)
        self.assertIn("ReadWritePaths=/var/lib/dcar-aigc/douyin-control", unit)
        self.assertEqual(unit.count("ReadWritePaths="), 1)
        self.assertIn("RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6", unit)
        self.assertIn("IPAddressDeny=any", unit)
        self.assertIn("IPAddressAllow=localhost", unit)
        self.assertIn("ProtectSystem=strict", unit)
        self.assertIn("CapabilityBoundingSet=\n", unit)
        self.assertIn(
            "EnvironmentFile=-/etc/dcar-aigc/douyin-stage1.env", unit
        )
        self.assertNotIn("Environment=DOUYIN_AUTHORIZATION_ENABLED=", unit)
        self.assertNotIn("Environment=DCAR_DOUYIN_PROVIDER=", unit)
        self.assertIn(
            "DCAR_DOUYIN_PROXY_URL=http://127.0.0.1:4176", unit
        )
        self.assertIn(
            "DCAR_DOUYIN_CLIENT_SECRET_FILE=/run/credentials/"
            "dcar-douyin-control.service/douyin-client-secret",
            unit,
        )
        for credential in (
            "douyin-edge-key",
            "douyin-machine-key",
            "douyin-fernet-keyring",
            "douyin-open-id-hmac-key",
            "douyin-client-secret",
        ):
            self.assertIn(f"LoadCredential={credential}:", unit)

    def test_douyin_egress_is_loopback_only_exact_and_fail_closed(self) -> None:
        unit = (
            ROOT / "deploy/server/systemd/dcar-douyin-egress.service"
        ).read_text(encoding="utf-8")
        config = (
            ROOT / "deploy/server/squid/dcar-douyin-egress.conf"
        ).read_text(encoding="utf-8")

        self.assertIn("User=proxy", unit)
        self.assertIn("Group=proxy", unit)
        self.assertIn("NoNewPrivileges=true", unit)
        self.assertIn("ProtectSystem=strict", unit)
        self.assertIn("CapabilityBoundingSet=", unit)
        self.assertIn("RestrictAddressFamilies=AF_INET AF_INET6", unit)
        self.assertIn(
            "ExecStart=/usr/sbin/squid -N -f /etc/squid/dcar-douyin-egress.conf",
            unit,
        )

        self.assertIn("http_port 127.0.0.1:4176", config)
        self.assertRegex(
            config,
            r"(?m)^acl douyin_openapi dstdomain -n open\.douyin\.com$",
        )
        self.assertRegex(config, r"(?m)^acl TLS_port port 443$")
        self.assertRegex(config, r"(?m)^acl CONNECT method CONNECT$")
        self.assertRegex(config, r"(?m)^http_access deny !CONNECT$")
        self.assertRegex(config, r"(?m)^http_access deny !TLS_port$")
        self.assertRegex(
            config,
            r"(?m)^http_access allow localhost CONNECT douyin_openapi TLS_port$",
        )
        self.assertRegex(config, r"(?m)^http_access deny all$")
        self.assertIn("cache deny all", config)
        self.assertIn("cache_mem 0 KB", config)
        self.assertIn("maximum_object_size 0 KB", config)
        self.assertNotIn("cache_dir", config)
        self.assertIn("access_log none", config)
        self.assertIn("cache_store_log none", config)
        self.assertNotIn(".open.douyin.com", config)

    def test_auth_unit_depends_on_control_and_uses_edge_credential(self) -> None:
        unit = (ROOT / "deploy/server/systemd/dcar-auth.service").read_text(
            encoding="utf-8"
        )
        self.assertRegex(unit, r"After=.*dcar-douyin-control\.service")
        self.assertRegex(unit, r"Wants=.*dcar-douyin-control\.service")
        self.assertIn(
            "DCAR_AUTH_DOUYIN_UPSTREAM=http://127.0.0.1:4175", unit
        )
        self.assertNotIn("%d/", unit)
        self.assertIn("LoadCredential=douyin-edge-key:", unit)

    def test_backup_unit_is_independent_bounded_and_source_read_only(self) -> None:
        service = (
            ROOT / "deploy/server/systemd/dcar-douyin-vault-backup.service"
        ).read_text(encoding="utf-8")
        timer = (
            ROOT / "deploy/server/systemd/dcar-douyin-vault-backup.timer"
        ).read_text(encoding="utf-8")
        self.assertNotIn("dcar-douyin-control.service", service)
        dependency_lines = {
            line.split("=", 1)[0]
            for line in service.splitlines()
            if "=" in line
        }
        self.assertTrue(
            dependency_lines.isdisjoint({"After", "Wants", "Requires"}),
            service,
        )
        self.assertIn("TimeoutStartSec=60s", service)
        self.assertIn("PrivateNetwork=true", service)
        self.assertIn("RestrictAddressFamilies=AF_UNIX", service)
        self.assertIn("CapabilityBoundingSet=CAP_DAC_READ_SEARCH", service)
        self.assertIn(
            "ReadOnlyPaths=/var/lib/dcar-aigc/douyin-control", service
        )
        self.assertIn(
            "ReadWritePaths=/var/backups/dcar-aigc/douyin-control", service
        )
        self.assertEqual(service.count("ReadWritePaths="), 1)
        self.assertNotIn("ConditionFileNotEmpty", service)
        self.assertIn("OnCalendar=hourly", timer)
        self.assertIn("Persistent=true", timer)
        self.assertNotIn("OnUnitActiveSec", timer)

    def test_nginx_callback_is_exact_quiet_limited_and_clears_trusted_headers(
        self,
    ) -> None:
        server = (ROOT / "deploy/server/nginx/dcar-proxy.conf").read_text(
            encoding="utf-8"
        )
        http = (ROOT / "deploy/server/nginx/dcar-http.conf").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "limit_req_zone $binary_remote_addr zone=dcar_douyin_callback:10m rate=5r/s;",
            http,
        )
        self.assertNotIn("limit_req_zone", server)
        exact = re.search(
            r"location = /dcar/oauth/douyin/callback \{(?P<body>.*?)\n\}",
            server,
            re.DOTALL,
        )
        self.assertIsNotNone(exact)
        body = exact.group("body") if exact else ""
        self.assertIn("limit_except GET { deny all; }", body)
        self.assertIn("limit_req zone=dcar_douyin_callback", body)
        self.assertIn("access_log off;", body)
        self.assertIn("error_log /var/log/nginx/dcar-douyin-callback-error.log crit;", body)
        self.assertIn("proxy_pass http://127.0.0.1:4173;", body)
        for header in (
            "Authorization",
            "X-Dcar-Authenticated-User",
            "X-Dcar-Session-Binding",
            "X-Dcar-Verified-Action",
            "X-Dcar-Edge-Key",
            "X-Dcar-Machine-Key",
        ):
            self.assertIn(f'proxy_set_header {header} "";', body)
            self.assertGreaterEqual(
                server.count(f'proxy_set_header {header} "";'), 2
            )

    def test_compose_separates_control_identity_vault_and_credentials(self) -> None:
        compose = (ROOT / "deploy/server/compose.yml").read_text(encoding="utf-8")
        control = compose.split("\n  control:\n", 1)[1].split("\n  auth:\n", 1)[0]
        auth = compose.split("\n  auth:\n", 1)[1]
        self.assertIn('user: "10002:10002"', control)
        self.assertIn('127.0.0.1:4175:4175', control)
        self.assertIn('DOUYIN_AUTHORIZATION_ENABLED: "0"', control)
        self.assertIn("DCAR_DOUYIN_PROVIDER: disabled", control)
        self.assertIn("read_only: true", control)
        self.assertIn("cap_drop:\n      - ALL", control)
        self.assertNotIn("CLIENT_SECRET", control)
        self.assertIn("target: /var/lib/dcar-aigc/douyin-control", control)
        for target in (
            "/run/secrets/douyin-edge-key",
            "/run/secrets/douyin-machine-key",
            "/run/secrets/douyin-fernet-keyring",
            "/run/secrets/douyin-open-id-hmac-key",
        ):
            self.assertIn(f"target: {target}\n        read_only: true", control)
        self.assertIn("control:\n        condition: service_healthy", auth)
        self.assertIn("DCAR_AUTH_DOUYIN_UPSTREAM: http://control:4175", auth)
        published = re.findall(r'"127\.0\.0\.1:(\d+):\1"', compose)
        self.assertCountEqual(published, ["4173", "4175"])
        self.assertNotIn('127.0.0.1:4174:4174', compose)
        self.assertNotIn('127.0.0.1:8765:8765', compose)

    def test_image_and_source_permissions_support_uid_10002(self) -> None:
        dockerfile = (ROOT / "deploy/server/Dockerfile.api").read_text(
            encoding="utf-8"
        )
        dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
        package = ROOT / "src/dcar_eval/dcar_douyin_control"
        self.assertIn("src/dcar_eval/dcar_douyin_control", dockerfile)
        self.assertIn("!src/dcar_eval/dcar_douyin_control/**", dockerignore)
        self.assertEqual(package.stat().st_mode & 0o777, 0o755)
        for path in package.rglob("*"):
            if path.is_dir():
                self.assertTrue(path.stat().st_mode & 0o005, path)
            elif path.suffix == ".py":
                self.assertTrue(path.stat().st_mode & 0o004, path)

    def test_backup_helper_is_executable_and_not_release_coupled(self) -> None:
        helper = (
            ROOT
            / "deploy/server/libexec/dcar-douyin-vault-backup.py"
        )
        self.assertTrue(os.access(helper, os.X_OK))
        source = helper.read_text(encoding="utf-8")
        self.assertIn("#!/usr/bin/env python3", source)
        self.assertNotIn("/var/www/dcar-aigc/current", source)
        self.assertNotIn("dcar_douyin_control", source)

    def test_server_docs_define_stage0_install_backup_and_rollback(self) -> None:
        readme = (ROOT / "deploy/server/README.md").read_text(encoding="utf-8")
        auth = (ROOT / "deploy/server/AUTH.md").read_text(encoding="utf-8")
        for text in (readme, auth):
            self.assertIn("dcar-douyin-control", text)
            self.assertIn("dcar-douyin-egress", text)
            self.assertIn("dcar-douyin-vault-backup", text)
            self.assertIn("DOUYIN_AUTHORIZATION_ENABLED=0", text)
            self.assertIn("Client Secret", text)
        self.assertIn("journal_mode=DELETE", readme)
        self.assertIn("dcar-http.conf", readme)
        self.assertIn(
            'restrict,port-forwarding,permitopen="127.0.0.1:4175"', readme
        )
        self.assertIn(
            "sudo install -d -o root -g root -m 0755 \\\n"
            "  /var/www/dcar-aigc /var/www/dcar-aigc/releases "
            "/var/www/dcar-aigc/runtime",
            readme,
        )
        self.assertIn(
            "sudo install -d -o root -g dcar-aigc -m 0751 "
            "/var/lib/dcar-aigc",
            readme,
        )
        self.assertIn("useradd --system --user-group", readme)
        self.assertIn("squid -k parse", readme)
        self.assertIn("open.douyin.com:443", readme)
        self.assertIn("DCAR_DOUYIN_PROXY_URL=http://127.0.0.1:4176", readme)
        self.assertIn("先恢复并 reload 前一版 Nginx", auth)


if __name__ == "__main__":
    unittest.main()
