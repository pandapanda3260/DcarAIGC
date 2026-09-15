import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import test from "node:test";

const { chromium } = createRequire(import.meta.url)("playwright");

const template = await readFile(new URL("../../../deploy/server/nginx/login.html", import.meta.url), "utf8");
const names = [
  "x",
  "someone@example.com",
  "  中文账号 🚗 + @ . / \\ & < > \" ' = ? # % _ -  ",
  " ",
  "第一行\n第二行",
  "🚗".repeat(150) + "a".repeat(400),
  '<script>window.usernameInjected = true</script><img src=x onerror="window.usernameInjected = true">',
];

test("registration and password login preserve unrestricted account names in the real browser", async () => {
  const browser = await chromium.launch({ headless: true, channel: process.env.DCAR_SMOKE_BROWSER_CHANNEL || undefined });
  try {
    const page = await browser.newPage();
    const requests = [];
    const errors = [];
    page.on("pageerror", (error) => errors.push(String(error)));
    await page.route("http://dcar-auth.test/**", async (route) => {
      const request = route.request();
      const pathname = new URL(request.url()).pathname;
      if (request.method() === "GET" && pathname.endsWith("/login")) return route.fulfill({ contentType: "text/html", body: template });
      if (request.method() === "POST") {
        requests.push({ pathname, body: new URLSearchParams(request.postData()) });
        return route.fulfill({ status: 400, contentType: "application/json", body: JSON.stringify({ detail: "test response" }) });
      }
      return route.fulfill({ status: 401, contentType: "application/json", body: "{}" });
    });

    for (const basePath of ["", "/dcar"]) {
      await page.goto(`http://dcar-auth.test${basePath}/login`);
      await page.click("#to-register");
      await page.fill("#reg-phone", "13800138000");
      await page.fill("#reg-password", "Test-password-42");
      await page.fill("#reg-code", "123456");
      for (const username of names) {
        await page.fill("#reg-username", username);
        assert.equal(await page.inputValue("#reg-username"), username);
        const count = requests.length;
        await page.click("#register-form .submit");
        await page.waitForFunction(() => !document.querySelector("#register-form .submit").disabled);
        assert.equal(requests.length, count + 1, `registration accepts ${JSON.stringify(username)}`);
        assert.equal(requests.at(-1).pathname, `${basePath}/auth/register`);
        assert.equal(requests.at(-1).body.get("username"), username);
        assert.equal(await page.locator("#reg-username-error").isVisible(), false);
      }
      await page.fill("#reg-username", "");
      const beforeEmptyRegistration = requests.length;
      await page.click("#register-form .submit");
      assert.equal(await page.locator("#reg-username-error").isVisible(), true);
      assert.equal(requests.length, beforeEmptyRegistration);

      await page.click("#register-form .to-login");
      await page.fill("#password", "Test-password-42");
      for (const username of names) {
        await page.fill("#username", username);
        const count = requests.length;
        await page.click("#submit-btn");
        await page.waitForFunction(() => !document.querySelector("#submit-btn").disabled);
        assert.equal(requests.length, count + 1, `login accepts ${JSON.stringify(username)}`);
        assert.equal(requests.at(-1).pathname, `${basePath}/auth/login`);
        assert.equal(requests.at(-1).body.get("username"), username);
      }
      await page.fill("#username", "");
      const beforeEmptyLogin = requests.length;
      await page.click("#submit-btn");
      assert.equal(await page.locator("#username-error").isVisible(), true);
      assert.equal(requests.length, beforeEmptyLogin);
      assert.equal(await page.evaluate(() => window.usernameInjected), undefined);
    }
    assert.deepEqual(errors, []);
  } finally {
    await browser.close();
  }
});
