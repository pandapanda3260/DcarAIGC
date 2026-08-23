from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AuthDeploymentContractTestCase(unittest.TestCase):
    def test_login_template_defaults_to_remember_and_aligns_password_actions(
        self,
    ) -> None:
        login = (ROOT / "deploy/server/nginx/login.html").read_text(encoding="utf-8")
        options = re.search(
            r'<div class="options-row">(?P<body>.*?)</div>', login, re.DOTALL
        )
        self.assertIsNotNone(options)
        options_body = options.group("body") if options else ""
        self.assertIn(
            '<input type="checkbox" id="remember" name="remember" checked>',
            options_body,
        )
        self.assertIn("<span>保持登录</span>", options_body)
        self.assertIn(
            '<button type="button" class="hint-link" id="forgot-btn">忘记密码？</button>',
            options_body,
        )
        self.assertGreater(
            options.start() if options else -1, login.index('id="password"')
        )
        self.assertNotIn('class="label-row"', login)
        self.assertRegex(
            login,
            r"\.options-row\s*\{[^}]*display:\s*flex;[^}]*align-items:\s*center;"
            r"[^}]*justify-content:\s*space-between;",
        )

    def test_login_template_handles_413_and_matches_gateway_error_copy(self) -> None:
        login = (ROOT / "deploy/server/nginx/login.html").read_text(encoding="utf-8")
        gateway = (ROOT / "src/dcar_eval/dcar_auth/gateway.py").read_text(
            encoding="utf-8"
        )
        self.assertRegex(
            login,
            r"else if \(response\.status === 413\) \{\s*"
            r'showBanner\("error", detail \? detail \+ "。" : '
            r'"登录信息太长，请刷新页面后重新输入。"\);',
        )
        for message in (
            "登录页面已失效，请刷新后重新登录",
            "登录信息太长，请刷新页面后重新输入",
            "尝试次数太多，请稍后再登录",
            "暂时无法登录，请稍后重试",
        ):
            with self.subTest(message=message):
                self.assertRegex(
                    gateway,
                    rf'\{{"detail":\s*"{re.escape(message)}"\}}',
                )
                self.assertIn(f'"{message}。"', login)

    def test_login_template_only_renders_the_fixed_douyin_session_notice(self) -> None:
        login = (ROOT / "deploy/server/nginx/login.html").read_text(encoding="utf-8")
        self.assertIn('params.get("notice")', login)
        self.assertIn('notice === "douyin-session-required"', login)
        self.assertIn(
            'showBanner("info", "登录状态已失效，请重新登录后再次发起授权。")',
            login,
        )
        self.assertNotIn("showBanner(\"info\", notice)", login)

    def test_compose_publishes_only_the_gateway_and_loopback_control(self) -> None:
        compose = (ROOT / "deploy/server/compose.yml").read_text(encoding="utf-8")
        self.assertIn("  auth:\n", compose)
        self.assertIn("dcar_auth.gateway:app", compose)
        self.assertIn('127.0.0.1:4173:4173', compose)
        self.assertIn('127.0.0.1:4175:4175', compose)
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
        self.assertIn("src/dcar_eval/dcar_douyin_control", api_image)
        self.assertIn("ARG PIP_INDEX_URL", api_image)
        self.assertIn("ARG PIP_EXTRA_INDEX_URL", api_image)
        self.assertIn("ARG PIP_FIND_LINKS", api_image)
        self.assertIn("ARG PIP_NO_INDEX", api_image)
        self.assertIn("ARG PIP_TRUSTED_HOST", api_image)
        self.assertNotIn("ENV PIP_INDEX_URL", api_image)
        self.assertNotIn("ENV PIP_EXTRA_INDEX_URL", api_image)
        self.assertNotIn("ENV PIP_FIND_LINKS", api_image)
        self.assertIn("!src/dcar_eval/dcar_auth/**", dockerignore)
        self.assertIn("!src/dcar_eval/dcar_douyin_control/**", dockerignore)
        self.assertIn("deploy/server/nginx/login.html", api_image)
        self.assertIn("EXPOSE 4174", web_image)
        self.assertIn("dcar_auth.gateway:app", unit)
        self.assertIn("ReadWritePaths=/var/lib/dcar-aigc/auth", unit)
        self.assertIn("proxy_pass http://127.0.0.1:4173", nginx)
        self.assertNotIn("auth_basic", nginx)
        self.assertNotIn("auth_request", nginx)


if __name__ == "__main__":
    unittest.main()
