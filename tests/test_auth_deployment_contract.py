from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AuthDeploymentContractTestCase(unittest.TestCase):
    def test_compose_uses_the_gateway_as_the_only_published_entry(self) -> None:
        compose = (ROOT / "deploy/server/compose.yml").read_text(encoding="utf-8")
        self.assertIn("  auth:\n", compose)
        self.assertIn("dcar_auth.gateway:app", compose)
        self.assertIn('127.0.0.1:4173:4173', compose)
        self.assertIn("DCAR_AUTH_WEB_UPSTREAM: http://web:4174", compose)
        self.assertIn("DCAR_AUTH_API_UPSTREAM: http://api:8765", compose)
        self.assertIn("DCAR_AUTH_SESSION_DB: /var/lib/dcar-aigc/auth/sessions.sqlite3", compose)
        self.assertIn("target: /run/secrets/dcar-htpasswd\n        read_only: true", compose)
        self.assertIn("target: /var/lib/dcar-aigc/auth\n        bind:", compose)
        self.assertIn("/dcar/auth/health", compose)
        self.assertNotIn('127.0.0.1:4174:4174', compose)
        self.assertNotIn('127.0.0.1:8765:8765', compose)

    def test_images_and_systemd_include_the_single_gateway_contract(self) -> None:
        api_image = (ROOT / "deploy/server/Dockerfile.api").read_text(
            encoding="utf-8"
        )
        web_image = (ROOT / "deploy/server/Dockerfile.web").read_text(
            encoding="utf-8"
        )
        unit = (ROOT / "deploy/server/systemd/dcar-auth.service").read_text(
            encoding="utf-8"
        )
        nginx = (ROOT / "deploy/server/nginx/dcar-proxy.conf").read_text(
            encoding="utf-8"
        )
        dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
        self.assertIn("src/dcar_eval/dcar_auth", api_image)
        self.assertIn("!src/dcar_eval/dcar_auth/**", dockerignore)
        self.assertIn("deploy/server/nginx/login.html", api_image)
        self.assertIn("EXPOSE 4174", web_image)
        self.assertIn("dcar_auth.gateway:app", unit)
        self.assertIn("ReadWritePaths=/var/lib/dcar-aigc/auth", unit)
        self.assertIn("proxy_pass http://127.0.0.1:4173", nginx)
        self.assertNotIn("auth_basic", nginx)
        self.assertNotIn("auth_request", nginx)


if __name__ == "__main__":
    unittest.main()
