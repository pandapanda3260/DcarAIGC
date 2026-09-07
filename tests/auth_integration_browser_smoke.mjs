import fs from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(new URL("../app/web/package.json", import.meta.url));
const { chromium } = require("playwright");

const BASE = process.env.DCAR_SMOKE_BASE_URL;
const GATEWAY_LOG = process.env.DCAR_SMOKE_GATEWAY_LOG;
if (!BASE || !GATEWAY_LOG) {
  throw new Error("DCAR_SMOKE_BASE_URL and DCAR_SMOKE_GATEWAY_LOG are required");
}
const PREFIX = new URL(BASE).pathname.replace(/\/$/, "");

const PASSWORD = "T3mp-Smoke-Passphrase!";
const UNICODE_PASSWORD = "🔑".repeat(64);
const RESET_PASSWORD = "🔐".repeat(64);
const results = [];
const pageErrors = [];
let executionError = false;
let logOffset = fs.statSync(GATEWAY_LOG).size;

function check(name, condition, detail = "") {
  results.push({ name, ok: Boolean(condition), detail: String(detail || "") });
  if (!condition) throw new Error(`${name}${detail ? `: ${detail}` : ""}`);
}

async function waitUntil(callback, label, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const result = await callback();
      if (result) return result;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`timed out waiting for ${label}${lastError ? `: ${lastError}` : ""}`);
}

async function nextSmsCode() {
  return waitUntil(async () => {
    const text = fs.readFileSync(GATEWAY_LOG, "utf8");
    const fresh = text.slice(logOffset);
    const matches = [...fresh.matchAll(/sms\[log\] phone=\S+ code=(\d{6})/g)];
    if (!matches.length) return "";
    logOffset = text.length;
    return matches[matches.length - 1][1];
  }, "a log-channel SMS code");
}

function trackPage(page) {
  page.on("pageerror", (error) => pageErrors.push(`pageerror: ${error}`));
  page.on("console", (message) => {
    if (message.type() === "error" && !/status of 4\d\d/.test(message.text())) {
      pageErrors.push(`console: ${message.text()}`);
    }
  });
}

async function authSession(page) {
  return page.evaluate(async (url) => {
    const response = await fetch(url);
    return { status: response.status, body: await response.json() };
  }, `${BASE}/auth/session`);
}

async function passwordLogin(page, username, password, returnTo = "/overview") {
  await page.goto(`${BASE}/login?return_to=${encodeURIComponent(PREFIX + returnTo)}`);
  await page.fill("#username", username);
  await page.fill("#password", password);
  await page.click("#submit-btn");
}

const browser = await chromium.launch({ headless: true });
try {
  // Registration creates an authenticated but unapproved session. Neither a
  // guessed page URL nor a direct API request may open the workbench.
  {
    const context = await browser.newContext();
    const page = await context.newPage();
    trackPage(page);
    await page.goto(`${BASE}/login?return_to=${encodeURIComponent("/overview")}`);
    await page.click("#to-register");
    await page.fill("#reg-username", "registered_smoke");
    await page.fill("#reg-phone", "13800138108");
    await page.fill("#reg-password", "🔑".repeat(7));
    await page.click("#register-form .submit");
    check("registration rejects seven Unicode code points", await page.locator("#reg-password-error").isVisible());
    await page.fill("#reg-password", "🔑".repeat(65));
    await page.click("#register-form .submit");
    check("registration rejects 65 Unicode code points", await page.locator("#reg-password-error").isVisible());
    await page.fill("#reg-password", UNICODE_PASSWORD);
    check("registration retains all 64 Unicode code points", await page.inputValue("#reg-password") === UNICODE_PASSWORD);
    await page.click("#register-form .code-btn");
    const code = await nextSmsCode();
    await page.fill("#reg-code", code);
    await page.click("#register-form .submit");
    await page.waitForURL(`${BASE}/pending-approval`);
    const session = await authSession(page);
    check("registration creates a new-user session", session.status === 200 && session.body.username === "registered_smoke" && session.body.role === "new_user", JSON.stringify(session));
    check("new user sees only the pending approval page", await page.getByRole("heading", { name: "等待管理员授权" }).isVisible() && await page.getByRole("navigation").count() === 0);
    const denied = await page.evaluate(async (url) => {
      const response = await fetch(url);
      return { status: response.status, body: await response.json() };
    }, `${BASE}/api/v8/smoke`);
    check("new-user API request is denied by the gateway", denied.status === 403 && denied.body.code === "approval_required" && !("upstream" in denied.body), JSON.stringify(denied));
    await page.goto(`${BASE}/overview`);
    await page.waitForURL(`${BASE}/pending-approval`);
    check("new user cannot bypass approval with a business URL", await page.getByRole("navigation").count() === 0);
    await page.getByRole("button", { name: "刷新权限" }).click();
    await waitUntil(() => page.getByRole("status").innerText().then((text) => text.includes("暂未获得授权")), "unapproved permission refresh");
    check("refresh keeps an unapproved user on the waiting page", page.url() === `${BASE}/pending-approval`);
    await context.close();
  }

  // Password login for the newly registered account.
  {
    const context = await browser.newContext();
    const page = await context.newPage();
    trackPage(page);
    await passwordLogin(page, "registered_smoke", UNICODE_PASSWORD);
    await page.waitForURL(`${BASE}/pending-approval`);
    const session = await authSession(page);
    check("password login preserves the new-user role", session.status === 200 && session.body.username === "registered_smoke" && session.body.role === "new_user", JSON.stringify(session));
    await context.close();
  }

  // Verification-code login uses a different phone so the real 60-second
  // per-phone send ledger stays enabled and the smoke remains fast.
  {
    const context = await browser.newContext();
    const page = await context.newPage();
    trackPage(page);
    await page.goto(`${BASE}/login?return_to=${encodeURIComponent("/overview")}`);
    await page.click("#login-mode-toggle");
    await page.fill("#login-phone", "13800138104");
    await page.click("#login-form .code-btn");
    const code = await nextSmsCode();
    await page.fill("#login-code", code);
    await page.click("#submit-btn");
    await page.waitForURL(`${BASE}/overview`);
    const session = await authSession(page);
    check("verification-code login succeeds", session.status === 200 && session.body.username === "code_smoke", JSON.stringify(session));
    await context.close();
  }

  // Password reset creates a new session and revokes the password session held
  // by another browser context.
  {
    const oldContext = await browser.newContext();
    const oldPage = await oldContext.newPage();
    trackPage(oldPage);
    await passwordLogin(oldPage, "reset_smoke", PASSWORD);
    await oldPage.waitForURL(`${BASE}/overview`);

    const resetContext = await browser.newContext();
    const resetPage = await resetContext.newPage();
    trackPage(resetPage);
    await resetPage.goto(`${BASE}/login?return_to=${encodeURIComponent("/overview")}`);
    await resetPage.click("#forgot-btn");
    await resetPage.fill("#reset-phone", "13800138105");
    await resetPage.click("#reset-form .code-btn");
    const code = await nextSmsCode();
    await resetPage.fill("#reset-code", code);
    await resetPage.click("#reset-form .submit");
    await resetPage.waitForSelector("#reset-step2:not([hidden])");
    await resetPage.fill("#reset-password", RESET_PASSWORD);
    check("reset retains all 64 Unicode code points", await resetPage.inputValue("#reset-password") === RESET_PASSWORD);
    await resetPage.click("#reset-form .submit");
    await resetPage.waitForURL(`${BASE}/overview`);
    const session = await authSession(resetPage);
    check("password reset logs in with a new session", session.status === 200 && session.body.username === "reset_smoke", JSON.stringify(session));
    const staleResponse = await oldPage.goto(`${BASE}/auth/session`);
    check("password reset revokes the old session", staleResponse?.status() === 401, staleResponse?.status());
    await resetContext.close();
    await oldContext.close();
  }

  // Disabled account and unadmitted registration phone both surface the real
  // gateway's 403 copy in the login-template banner.
  {
    const context = await browser.newContext();
    const page = await context.newPage();
    trackPage(page);
    await passwordLogin(page, "disabled_smoke", PASSWORD);
    await page.waitForSelector("#login-form .banner:not([hidden])");
    const disabledCopy = await page.locator("#login-form .banner-text").innerText();
    check("disabled-account 403 is shown in the banner", disabledCopy === "该账号已停用。", disabledCopy);
    await page.click("#to-register");
    await page.fill("#reg-phone", "13800138199");
    await page.click("#register-form .code-btn");
    await page.waitForSelector("#register-form .banner:not([hidden])");
    const deniedCopy = await page.locator("#register-form .banner-text").innerText();
    check("unadmitted phone is rejected in the banner", deniedCopy === "该手机号未获授权。", deniedCopy);
    check("failed code send re-enables its button", !(await page.locator("#register-form .code-btn").isDisabled()));
    await context.close();
  }

  // Superadmin: real UsersPage, role mutation, delete action, focus trap,
  // focus restoration, and latest busy state during an in-flight request.
  {
    const context = await browser.newContext();
    const page = await context.newPage();
    trackPage(page);
    await passwordLogin(page, "super_smoke", PASSWORD, "/users");
    await page.waitForURL(`${BASE}/users`);
    await page.waitForSelector('[data-username="manager_smoke"]');
    check("superadmin sees user-management navigation", await page.getByRole("link", { name: "用户权限" }).isVisible());
    check("UsersPage renders seeded role labels", await page.getByText("超级管理员", { exact: true }).first().isVisible() && await page.getByText("运营人员", { exact: true }).first().isVisible());

    const row = page.locator('[data-username="manager_smoke"]');
    const editButton = row.getByRole("button", { name: "修改" });
    await editButton.click();
    const dialog = page.getByRole("dialog", { name: "修改用户" });
    await dialog.waitFor({ state: "visible" });
    check("role editor offers four roles including new user", (await dialog.getByLabel("权限等级").locator("option").evaluateAll((options) => options.map((option) => option.value).join(","))) === "superadmin,admin,operator,new_user");
    await waitUntil(
      () => dialog.getByRole("button", { name: "关闭" }).evaluate((element) => document.activeElement === element),
      "dialog initial focus",
    );
    check("dialog initially focuses its close button", true);
    await page.keyboard.press("Shift+Tab");
    check(
      "Shift+Tab wraps from first to last focusable control",
      await dialog.getByRole("button", { name: "保存", exact: true }).evaluate((element) => document.activeElement === element),
    );
    await page.keyboard.press("Tab");
    check(
      "Tab wraps from last to first focusable control",
      await dialog.getByRole("button", { name: "关闭" }).evaluate((element) => document.activeElement === element),
    );
    await page.keyboard.press("Escape");
    await dialog.waitFor({ state: "hidden" });
    await waitUntil(() => editButton.evaluate((element) => document.activeElement === element), "trigger focus restoration");
    check("closing the dialog restores trigger focus", true);

    await editButton.click();
    await dialog.waitFor({ state: "visible" });
    await dialog.getByLabel("权限等级").selectOption("admin");
    await dialog.getByLabel("新密码").fill(UNICODE_PASSWORD);
    let releaseUpdate;
    let markUpdateStarted;
    const updateStarted = new Promise((resolve) => { markUpdateStarted = resolve; });
    const updateReleased = new Promise((resolve) => { releaseUpdate = resolve; });
    await page.route("**/auth/users/update", async (route) => {
      markUpdateStarted();
      await updateReleased;
      await route.continue();
    }, { times: 1 });
    const updateResponse = page.waitForResponse((response) => response.url().endsWith("/auth/users/update"));
    await dialog.getByRole("button", { name: "保存", exact: true }).click();
    await updateStarted;
    await dialog.getByRole("button", { name: "保存中" }).waitFor({ state: "visible" });
    await page.keyboard.press("Escape");
    check("Escape cannot close a submitting dialog", await dialog.isVisible());
    check("submitting dialog disables close", await dialog.getByRole("button", { name: "关闭" }).isDisabled());
    releaseUpdate();
    const saved = await updateResponse;
    check("role update reaches the real gateway", saved.status() === 200, saved.status());
    await dialog.waitFor({ state: "hidden" });
    await waitUntil(() => row.getByText("管理员", { exact: true }).isVisible(), "updated role in table");
    check("updated role is rendered by UsersPage", true);

    // Approve the registered account through the real role editor, then revoke
    // access while that user's business page and query cache are still open.
    const pendingContext = await browser.newContext();
    const pendingPage = await pendingContext.newPage();
    trackPage(pendingPage);
    await passwordLogin(pendingPage, "registered_smoke", UNICODE_PASSWORD);
    await pendingPage.waitForURL(`${BASE}/pending-approval`);
    const registeredRow = page.locator('[data-username="registered_smoke"]');
    check("registered account is listed as new user", await registeredRow.getByText("新用户", { exact: true }).isVisible());
    await registeredRow.getByRole("button", { name: "修改" }).click();
    await dialog.getByLabel("权限等级").selectOption("operator");
    const approvalResponse = page.waitForResponse((response) => response.url().endsWith("/auth/users/update"));
    await dialog.getByRole("button", { name: "保存", exact: true }).click();
    check("superadmin authorizes the new account via UsersPage", (await approvalResponse).status() === 200);
    await dialog.waitFor({ state: "hidden" });
    await waitUntil(() => registeredRow.getByText("运营人员", { exact: true }).isVisible(), "approved role in table");
    await pendingPage.getByRole("button", { name: "刷新权限" }).click();
    await pendingPage.waitForURL(`${BASE}/overview`);
    await pendingPage.waitForSelector("main[data-section='overview']");
    check("approved user enters the workbench without logging in again", (await authSession(pendingPage)).body.role === "operator");
    const upstream = await pendingPage.evaluate(async (url) => {
      const response = await fetch(url);
      return { status: response.status, body: await response.json() };
    }, `${BASE}/api/v8/smoke`);
    check("approved user reaches the upstream with its authenticated identity", upstream.status === 200 && upstream.body.upstream === "api" && upstream.body.user === "registered_smoke", JSON.stringify(upstream));
    await pendingPage.goto(`${BASE}/contents`);
    await pendingPage.getByPlaceholder("内容编号、标题、账号编号、昵称或链接").waitFor({ state: "visible" });
    await registeredRow.getByRole("button", { name: "修改" }).click();
    await dialog.getByLabel("权限等级").selectOption("new_user");
    const revocationResponse = page.waitForResponse((response) => response.url().endsWith("/auth/users/update"));
    await dialog.getByRole("button", { name: "保存", exact: true }).click();
    check("superadmin can return an account to new user", (await revocationResponse).status() === 200);
    await dialog.waitFor({ state: "hidden" });
    const deniedSearch = pendingPage.waitForResponse((response) => response.url().endsWith("/api/v8/contents/search") && response.status() === 403);
    await pendingPage.getByPlaceholder("内容编号、标题、账号编号、昵称或链接").fill("permission-recheck");
    await pendingPage.getByRole("button", { name: "搜索", exact: true }).click();
    check("live business request observes approval revocation", (await (await deniedSearch).json()).code === "approval_required");
    await pendingPage.waitForURL(`${BASE}/pending-approval`);
    check("revoked user leaves its cached business page without exposing navigation", await pendingPage.getByRole("heading", { name: "等待管理员授权" }).isVisible() && await pendingPage.getByRole("navigation").count() === 0 && await pendingPage.locator("main[data-section]").count() === 0);
    await pendingContext.close();

    const deleteRow = page.locator('[data-username="delete_smoke"]');
    await deleteRow.getByRole("button", { name: "删除" }).click();
    const deleteDialog = page.getByRole("dialog", { name: "删除用户" });
    await deleteDialog.waitFor({ state: "visible" });
    const deleteResponse = page.waitForResponse((response) => response.url().endsWith("/auth/users/delete"));
    await deleteDialog.getByRole("button", { name: "确认删除" }).click();
    const deleted = await deleteResponse;
    check("delete reaches the real gateway", deleted.status() === 200, deleted.status());
    await waitUntil(() => deleteRow.count().then((count) => count === 0), "deleted user to leave table");
    check("deleted user leaves the table", true);
    await context.close();
  }

  // The promoted admin may enter /users but cannot act on a superadmin.
  {
    const context = await browser.newContext();
    const page = await context.newPage();
    trackPage(page);
    await passwordLogin(page, "manager_smoke", UNICODE_PASSWORD, "/users");
    await page.waitForURL(`${BASE}/users`);
    await page.waitForSelector('[data-username="super_smoke"]');
    const superRow = page.locator('[data-username="super_smoke"]');
    check("promoted admin can enter UsersPage", await page.getByRole("link", { name: "用户权限" }).isVisible());
    check("admin cannot modify a superadmin", (await superRow.getByRole("button").count()) === 0);
    const selfRow = page.locator('[data-username="manager_smoke"]');
    check("user cannot delete itself in the page", (await selfRow.getByRole("button", { name: "删除" }).count()) === 0);

    // Revoke this live browser's role through another real superadmin session,
    // then exercise its still-open dialog rather than a full page navigation.
    await page.locator('[data-username="code_smoke"]').getByRole("button", { name: "修改" }).click();
    const supervisorContext = await browser.newContext();
    const supervisorPage = await supervisorContext.newPage();
    trackPage(supervisorPage);
    await passwordLogin(supervisorPage, "super_smoke", PASSWORD);
    await supervisorPage.waitForURL(`${BASE}/overview`);
    const demoted = await supervisorPage.evaluate(async (url) => {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Dcar-Request": "user-update" },
        body: JSON.stringify({ username: "manager_smoke", phone: "13800138102", role: "operator", password: "" }),
      });
      return response.status;
    }, `${BASE}/auth/users/update`);
    check("another superadmin revokes the live manager role", demoted === 200, demoted);
    await page.getByRole("dialog", { name: "修改用户" }).getByRole("button", { name: "保存", exact: true }).click();
    await page.waitForURL(`${BASE}/overview`);
    check("revoked manager leaves the cached user page on its next operation", page.url() === `${BASE}/overview`);
    await waitUntil(() => page.locator("main[data-section='overview']").isVisible(), "overview after role revocation");
    check("revoked manager no longer sees management navigation", (await page.getByRole("link", { name: "用户权限" }).count()) === 0);
    await supervisorContext.close();
    await context.close();
  }

  // Operator: the gateway, not just the hidden sidebar, enforces both the page
  // redirect and JSON endpoint 403.
  {
    const context = await browser.newContext();
    const page = await context.newPage();
    trackPage(page);
    await passwordLogin(page, "operator_smoke", PASSWORD, "/users");
    await page.waitForURL(`${BASE}/overview`);
    await page.waitForSelector("main[data-section='overview']");
    check("operator is redirected away from UsersPage", page.url() === `${BASE}/overview`, page.url());
    check("operator does not see user-management navigation", (await page.getByRole("link", { name: "用户权限" }).count()) === 0);
    const denied = await page.evaluate(async (url) => {
      const response = await fetch(url);
      return { status: response.status, body: await response.json() };
    }, `${BASE}/auth/users`);
    check("operator receives 403 from user-management API", denied.status === 403 && denied.body.code === "forbidden", JSON.stringify(denied));
    await context.close();
  }

  check("browser pages emitted no uncaught errors", pageErrors.length === 0, pageErrors.join(" | "));
} catch (error) {
  executionError = true;
  console.error(error instanceof Error ? error.stack : error);
} finally {
  await browser.close();
}

const failed = results.filter((result) => !result.ok);
console.log(JSON.stringify({ passed: results.length - failed.length, failed: failed.length }, null, 0));
for (const result of results) {
  console.log(`${result.ok ? "PASS" : "FAIL"} ${result.name}${result.detail ? ` -- ${result.detail}` : ""}`);
}
if (executionError || failed.length || results.length < 30) process.exitCode = 1;
