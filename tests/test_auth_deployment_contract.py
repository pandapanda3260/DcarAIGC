from __future__ import annotations

import re
import shutil
import subprocess
import unittest
from pathlib import Path
from posixpath import normpath


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
            r'showBanner\((?:loginForm, )?"error", detail \? detail \+ "。" : '
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
                    rf'(\{{"detail":\s*"{re.escape(message)}"\}}|"[a-z_]+":\s*"{re.escape(message)}",)',
                )
                self.assertIn(f'"{message}。"', login)

    def test_login_template_only_renders_the_fixed_douyin_session_notice(self) -> None:
        login = (ROOT / "deploy/server/nginx/login.html").read_text(encoding="utf-8")
        self.assertIn('params.get("notice")', login)
        self.assertIn('notice === "douyin-session-required"', login)
        self.assertRegex(
            login,
            r'showBanner\((?:loginForm, )?"info", "登录状态已失效，请重新登录后再次发起授权。"\)',
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
        self.assertIn("DCAR_AUTH_SMS_PROVIDER: tencent", compose)
        self.assertIn("DCAR_AUTH_SMS_CREDENTIALS_FILE: /run/secrets/sms-tencent", compose)
        self.assertIn("DCAR_AUTH_PEPPER_FILE: /run/secrets/auth-pepper", compose)
        self.assertIn("target: /run/secrets/sms-tencent\n        read_only: true", compose)
        self.assertIn("target: /run/secrets/auth-pepper\n        read_only: true", compose)
        self.assertNotIn("htpasswd", compose)
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
        self.assertIn(
            "src/dcar_eval/tikhub_config.py /app/src/dcar_eval/tikhub_config.py",
            api_image,
        )
        self.assertIn("!src/dcar_eval/tikhub_config.py", dockerignore)
        self.assertIn("EXPOSE 4174", web_image)
        self.assertIn("dcar_auth.gateway:app", unit)
        self.assertIn("ReadWritePaths=/var/lib/dcar-aigc/auth", unit)
        self.assertIn("Environment=DCAR_AUTH_SMS_PROVIDER=tencent", unit)
        self.assertIn(
            "LoadCredential=sms-tencent:/etc/dcar-aigc/credentials/sms-tencent", unit
        )
        self.assertIn(
            "LoadCredential=auth-pepper:/etc/dcar-aigc/credentials/auth-pepper", unit
        )
        self.assertNotIn("htpasswd", unit)
        self.assertIn("proxy_pass http://127.0.0.1:4173", nginx)
        self.assertNotIn("auth_basic", nginx)
        self.assertNotIn("auth_request", nginx)
        self.assertTrue(
            (ROOT / "src/dcar_eval/dcar_auth/common_passwords.txt").is_file()
        )
        self.assertTrue((ROOT / "src/dcar_eval/dcar_auth/admin.py").is_file())
        self.assertFalse((ROOT / "scripts/create_local_auth_user.py").exists())

    def test_backup_units_and_helper_are_installed_together(self) -> None:
        service = (ROOT / "deploy/server/systemd/dcar-auth-backup.service").read_text(
            encoding="utf-8"
        )
        timer = (ROOT / "deploy/server/systemd/dcar-auth-backup.timer").read_text(
            encoding="utf-8"
        )
        helper = ROOT / "deploy/server/libexec/dcar-auth-backup.py"
        readme = (ROOT / "deploy/server/README.md").read_text(encoding="utf-8")
        self.assertIn(
            "ExecStart=/usr/local/libexec/dcar-auth-backup --source "
            "/var/lib/dcar-aigc/auth/sessions.sqlite3 --backup-dir "
            "/var/backups/dcar-aigc/auth --keep-days 30 --expect-user-version 3",
            service,
        )
        self.assertIn("ReadOnlyPaths=/var/lib/dcar-aigc/auth", service)
        self.assertIn("ReadWritePaths=/var/backups/dcar-aigc/auth", service)
        self.assertIn("PrivateNetwork=true", service)
        self.assertIn("OnCalendar=*-*-* 03:30:00", timer)
        self.assertIn("Persistent=true", timer)
        self.assertTrue(helper.is_file())
        self.assertTrue(helper.stat().st_mode & 0o111)
        self.assertIn("dcar-auth-backup.timer", readme)
        self.assertIn("/usr/local/libexec/dcar-auth-backup", readme)

    def test_login_template_renders_three_views_with_the_new_contract(self) -> None:
        login = (ROOT / "deploy/server/nginx/login.html").read_text(encoding="utf-8")
        gateway = (ROOT / "src/dcar_eval/dcar_auth/gateway.py").read_text(
            encoding="utf-8"
        )
        self.assertEqual(login.count("<form "), 3)
        for form_id in ("login-form", "register-form", "reset-form"):
            self.assertIn(f'id="{form_id}"', login)
        for text in ("完成注册并登录", "确认修改并登录", "验 证", "登 录", "验证码登录", "注册账号", "返回登录"):
            self.assertIn(text, login)
        for marker in ("login-code", "register", "reset-verify", "reset-confirm", "code"):
            self.assertIn(f'"{marker}": ', login)
        for endpoint in ("/auth/code", "/auth/login/code", "/auth/register", "/auth/reset/verify", "/auth/reset/confirm"):
            self.assertIn(f'"{endpoint}"', login)
            self.assertIn(f'"{endpoint}": ', gateway)
        self.assertEqual(login.count('class="code-btn"'), 3)
        self.assertNotIn("secure-note", login)
        self.assertNotIn("请使用管理员分配的运营账号", login)
        # Routine login/registration copy stays self-service; only an absent
        # backend route gets the actionable administrator recovery message.
        self.assertNotIn(
            "联系管理员",
            login.replace("验证码服务尚未就绪，请联系管理员更新登录服务。", ""),
        )
        self.assertNotIn("验证码已发送", login)
        for message in ("账号为 4–32 位字母、数字或下划线", "密码长度需为 8–64 位", "验证码为 6 位数字", "手机号格式不正确"):
            self.assertIn(message, login)

    def test_registration_copy_preserves_login_and_loading_labels(self) -> None:
        login = (ROOT / "deploy/server/nginx/login.html").read_text(encoding="utf-8")
        self.assertIn('<label for="reg-username">账号名称</label>', login)
        self.assertRegex(
            login,
            r'<input id="reg-username"[^>]*placeholder="请输入账号名称"',
        )
        self.assertIn(
            'requireValue(inputs[0], "请输入账号名称", USERNAME_PATTERN,', login
        )
        self.assertIn('<label for="username">账号</label>', login)
        self.assertRegex(
            login, r'<input id="username"[^>]*placeholder="请输入账号"'
        )
        self.assertIn('<span class="submit-text">完成注册并登录</span>', login)
        self.assertIn(
            'setLoading(form, true, "完成注册并登录", "注册中…");', login
        )
        # Both response-error and network-error paths must restore the new copy.
        self.assertEqual(
            login.count('setLoading(form, false, "完成注册并登录", "");'), 2
        )
        self.assertNotIn('"注册并登录"', login)
        self.assertNotIn('>注册并登录<', login)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for browser event regression")
    def test_code_button_distinguishes_missing_routes_and_releases_busy_state(self) -> None:
        script = r"""
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const html = fs.readFileSync(process.argv[1], "utf8");
const start = html.indexOf("  /* ---- 获取验证码：");
const end = html.indexOf("  /* ---- 登录 ---- */", start);
assert.ok(start >= 0 && end > start, "code-button event source must exist");
const source = html.slice(start, end);

async function check(purpose, status, detail, expected, networkError = false) {
  let click;
  let timers = 0;
  let focused = false;
  let banner;
  let request;
  const form = {};
  const phone = { value: "13800138000", focus() {} };
  const button = {
    disabled: false,
    textContent: "获取验证码",
    closest: () => form,
    getAttribute: key => key === "data-purpose" ? purpose : "phone",
    parentNode: { querySelector: () => ({ focus() { focused = true; } }) },
    addEventListener(type, listener) {
      assert.equal(type, "click");
      click = listener;
    }
  };
  vm.runInNewContext(source, {
    document: { querySelectorAll: () => [button] },
    window: { setInterval() { timers++; return 1; }, clearInterval() {} },
    COUNTDOWN_SECONDS: 60,
    countdowns: [],
    PHONE_PATTERN: /^1[3-9][0-9]{9}$/,
    IS_PREVIEW: false,
    URLSearchParams,
    byId: () => phone,
    hideBanner() { banner = undefined; },
    requireValue: () => true,
    showBanner(target, kind, text) {
      assert.equal(target, form);
      assert.equal(kind, "error");
      banner = text;
    },
    detailOf: (result, fallback) => result.data?.detail || fallback,
    postForm(marker, body) {
      request = { marker, phone: body.get("phone"), purpose: body.get("purpose") };
      return networkError
        ? Promise.reject(new Error("offline"))
        : Promise.resolve({ response: { status, ok: status === 200 }, data: { detail } });
    }
  });
  click();
  assert.equal(button.disabled, true, "button is disabled while request is pending");
  await new Promise(setImmediate);
  assert.deepEqual(request, { marker: "code", phone: phone.value, purpose });
  assert.equal(banner, expected);
  assert.equal(timers, status === 200 ? 1 : 0, "failed sends never start a countdown");
  assert.equal(button.disabled, status === 200, "failed sends restore the button");
  assert.equal(button.textContent, status === 200 ? "60s" : "获取验证码");
  assert.equal(focused, status === 200);
}

(async () => {
  for (const purpose of ["register", "login", "reset"]) {
    for (const status of [404, 405]) {
      await check(purpose, status, "Not Found", "验证码服务尚未就绪，请联系管理员更新登录服务。");
    }
    await check(purpose, 503, "", "验证码发送失败，请稍后重试。");
    await check(purpose, 429, "请求太频繁，请稍后重试", "请求太频繁，请稍后重试。");
    await check(purpose, 400, "手机号格式不正确", "手机号格式不正确。");
    await check(purpose, 0, "", "无法连接到服务器，请检查网络后重试。", true);
    await check(purpose, 200, "", undefined);
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
"""
        result = subprocess.run(
            [shutil.which("node") or "node", "-e", script, str(ROOT / "deploy/server/nginx/login.html")],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_web_image_includes_the_active_selling_point_query_build_input(self) -> None:
        web_image = (ROOT / "deploy/server/Dockerfile.web").read_text(
            encoding="utf-8"
        )
        page = (ROOT / "app/web/app/selling-points/SellingPointsPage.tsx").read_text(
            encoding="utf-8"
        )
        query_import = re.search(
            r'import \{ activeSellingPointsQueryOptions \} from "([^"]+)";', page
        )
        self.assertIsNotNone(query_import)
        self.assertIn("useQuery(activeSellingPointsQueryOptions())", page)
        relative_path = query_import.group(1) if query_import else ""
        container_path = normpath(f"/app/app/selling-points/{relative_path}")
        self.assertEqual(container_path, "/app/app/lib/queries")
        query_source = (ROOT / "app/web" / f"{container_path.removeprefix('/app/')}.ts").read_text(
            encoding="utf-8"
        )
        active_query = re.search(
            r"export function activeSellingPointsQueryOptions\(\) \{(?P<body>.*?)\n\}",
            query_source,
            re.DOTALL,
        )
        self.assertIsNotNone(active_query)
        self.assertIn(
            'readQueryJson<SellingPointResponse>("/api/v8/selling-points")',
            active_query.group("body") if active_query else "",
        )
        copy_instruction = "COPY app/web/ ./"
        self.assertIn(copy_instruction, web_image)
        self.assertLess(
            web_image.index(copy_instruction),
            web_image.index("RUN node ./node_modules/vinext/dist/cli.js build"),
        )
        self.assertIn(
            "!app/web/**", (ROOT / ".dockerignore").read_text(encoding="utf-8")
        )


if __name__ == "__main__":
    unittest.main()
