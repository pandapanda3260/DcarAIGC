import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";
import ts from "typescript";
import { QueryClient } from "@tanstack/react-query";

let moduleId = 0;
async function freshApi(basePath = "") {
  const previousBasePath = process.env.NEXT_PUBLIC_DCAR_BASE_PATH;
  process.env.NEXT_PUBLIC_DCAR_BASE_PATH = basePath;
  const url = new URL(`../app/lib/api.ts?approval-test=${++moduleId}`, import.meta.url);
  try {
    return { api: await import(url.href), url: url.href };
  } finally {
    if (previousBasePath === undefined) delete process.env.NEXT_PUBLIC_DCAR_BASE_PATH;
    else process.env.NEXT_PUBLIC_DCAR_BASE_PATH = previousBasePath;
  }
}

async function loadModule(path, replacements) {
  const source = await readFile(new URL(path, import.meta.url), "utf8");
  const output = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
  }).outputText.replace(/from (["'])([^"']+)\1/g, (_match, _quote, specifier) => {
    const resolved = replacements[specifier] ?? import.meta.resolve(specifier);
    return `from ${JSON.stringify(resolved)}`;
  });
  return import(`data:text/javascript;base64,${Buffer.from(output).toString("base64")}`);
}

function installBrowser(t, { pathname = "/contents", search = "" } = {}) {
  const oldWindow = globalThis.window;
  const oldFetch = globalThis.fetch;
  const destinations = [];
  globalThis.window = { location: { pathname, search, replace: (path) => destinations.push(path) } };
  t.after(() => {
    globalThis.fetch = oldFetch;
    if (oldWindow === undefined) delete globalThis.window;
    else globalThis.window = oldWindow;
  });
  return destinations;
}

async function expectWaitingForNavigation(promise) {
  assert.equal(await Promise.race([
    promise.then(() => "resolved", () => "rejected"),
    new Promise((resolve) => setImmediate(() => resolve("navigating"))),
  ]), "navigating");
}

test("approval denial clears the browser client and reloads its gateway page once under either base path", async (t) => {
  const destinations = installBrowser(t);
  for (const basePath of ["", "/dcar"]) {
    globalThis.window.location.pathname = `${basePath}/contents`;
    const { api, url } = await freshApi(basePath);
    const browserQueryRuntime = `data:text/javascript,${encodeURIComponent(
      `export { QueryClient } from ${JSON.stringify(import.meta.resolve("@tanstack/react-query"))}; export const isServer = false;`,
    )}`;
    const { getQueryClient } = await loadModule("../app/lib/queryClient.ts", {
      "./api": url,
      "@tanstack/react-query": browserQueryRuntime,
    });
    const client = getQueryClient();
    client.setQueryData(["contents"], { items: [{ title: "previously authorized content" }] });
    client.setQueryData(["auth", "users"], { items: [{ username: "existing-user" }] });
    assert.equal(api.handleApprovalRequired(403, "forbidden"), false);
    assert.equal(client.getQueryCache().getAll().length, 2);

    const before = destinations.length;
    assert.equal(api.handleApprovalRequired(403, "approval_required"), true);
    assert.equal(client.getQueryCache().getAll().length, 0);
    assert.equal(destinations.at(-1), `${basePath}/contents`);
    api.handleApprovalRequired(403, "approval_required");
    assert.equal(destinations.length, before + 1);
    assert.equal(getQueryClient(), client);
  }
});

test("JSON and downloads wait for the gateway page reload without surfacing ordinary operation errors", async (t) => {
  const destinations = installBrowser(t);
  for (const kind of ["json", "download"]) {
    const { api } = await freshApi();
    globalThis.fetch = async () => Response.json({ code: "approval_required" }, { status: 403 });
    const request = kind === "json" ? api.readJson("/api/v8/contents/search") : api.readDownload("/report", {
      contentTypes: ["application/pdf"], fallbackFilename: "report.pdf",
    });
    await expectWaitingForNavigation(request);
    assert.equal(destinations.at(-1), "/contents");
  }
});

test("revocation prevents late successes and new requests from repopulating protected data", async (t) => {
  installBrowser(t);
  const { api } = await freshApi();
  const client = new QueryClient();
  api.setSessionDataClearer(() => client.clear());
  let release;
  let fetches = 0;
  globalThis.fetch = async () => {
    fetches += 1;
    return new Promise((resolve) => { release = resolve; });
  };
  const inflight = api.readJson("/api/v8/contents/search").then((data) => client.setQueryData(["contents"], data));
  api.handleApprovalRequired(403, "approval_required");
  release(Response.json({ items: [{ title: "stale data" }] }));
  await expectWaitingForNavigation(inflight);
  await expectWaitingForNavigation(api.readJson("/api/v8/accounts/search"));
  assert.equal(fetches, 1);
  assert.equal(client.getQueryData(["contents"]), undefined);
});

test("approved roles retain access, while a session downgrade immediately reloads the gateway shell", async (t) => {
  const destinations = installBrowser(t, { pathname: "/dcar/accounts" });
  const { api } = await freshApi("/dcar");
  for (const role of ["operator", "admin", "superadmin", undefined]) {
    const session = { authenticated: true, username: "test-user", role };
    assert.equal(api.requireApprovedSession(session), session);
  }
  assert.deepEqual(destinations, []);
  await expectWaitingForNavigation(api.requireApprovedSession({ role: "new_user" }));
  assert.deepEqual(destinations, ["/dcar/accounts"]);
});

test("approval reload keeps workbench navigation state but strips Flight request markers", async (t) => {
  const destinations = installBrowser(t);
  for (const basePath of ["", "/dcar"]) {
    for (const section of ["overview", "contents", "accounts", "selling-points", "spu-audience", "tasks"]) {
      const { api } = await freshApi(basePath);
      const pathname = `${basePath}/${section}/`;
      globalThis.window.location.pathname = pathname;
      globalThis.window.location.search = "?platform=douyin&_rsc=private-flight&_data=loader&page=2";
      assert.equal(api.handleApprovalRequired(403, "approval_required"), true);
      assert.equal(destinations.at(-1), `${pathname}?platform=douyin&page=2`);
    }
  }
});

test("approval reload falls back to the base-path overview for non-workbench routes", async (t) => {
  const destinations = installBrowser(t);
  for (const [basePath, pathname] of [
    ["", "/users"], ["", "/api/v8/contents/search"],
    ["/dcar", "/dcar/users"], ["/dcar", "/contents"],
    ["/dcar", "/dcar-other/contents"], ["/dcar", "/dcar/accounts/export"],
  ]) {
    const { api } = await freshApi(basePath);
    globalThis.window.location.pathname = pathname;
    globalThis.window.location.search = "?return_to=%2Fusers&_rsc=private-flight";
    assert.equal(api.handleApprovalRequired(403, "approval_required"), true);
    assert.equal(destinations.at(-1), `${basePath}/overview`);
  }
});

test("session refresh observes revocation on focus, reentry and every 30 seconds", async () => {
  const { url } = await freshApi();
  const { sessionQueryOptions } = await loadModule("../app/lib/queries.ts", {
    "./api": url,
    "./queryContracts": new URL("../app/lib/queryContracts.ts", import.meta.url).href,
    "./features": new URL("../app/lib/features.ts", import.meta.url).href,
  });
  const options = sessionQueryOptions();
  assert.equal(options.refetchInterval, 30_000);
  assert.equal(options.refetchOnWindowFocus, "always");
  assert.equal(options.refetchOnMount, "always");
});

test("an ordinary forbidden API response does not force the approval shell reload", async (t) => {
  const destinations = installBrowser(t);
  const { api } = await freshApi();
  globalThis.fetch = async () => Response.json({ code: "forbidden" }, { status: 403 });
  await assert.rejects(api.readJson("/auth/users"), (error) => (
    error instanceof api.ApiRequestError && error.status === 403 && error.code === "forbidden"
  ));
  assert.deepEqual(destinations, []);
});

test("an in-flight permission check cannot navigate ahead of logout", async () => {
  const source = await readFile(new URL("../../../src/dcar_eval/dcar_auth/gateway.py", import.meta.url), "utf8");
  const script = source.match(/<script>\nconst basePath=__BASE_PATH_JSON__;([\s\S]*?)<\/script>/)?.[1];
  assert.ok(script);
  for (const delayedStage of ["response", "json"]) {
    const destinations = [];
    const label = { textContent: "刷新权限" };
    const elements = {
      status: { textContent: "" },
      refresh: { disabled: false, querySelector: () => label },
      logout: { disabled: false },
    };
    let releaseSession, releaseLogout;
    const pendingSession = new Promise((resolve) => { releaseSession = resolve; });
    const pendingLogout = new Promise((resolve) => { releaseLogout = resolve; });
    const approved = { authenticated: true, role: "operator" };
    const runtime = {
      document: { getElementById: (id) => elements[id], addEventListener() {}, visibilityState: "visible" },
      window: { addEventListener() {} },
      location: { reload: () => destinations.push("reload"), replace: (url) => destinations.push(url), assign: (url) => destinations.push(url) },
      fetch: async (url) => url.endsWith("/logout") ? pendingLogout : delayedStage === "response"
        ? pendingSession : { ok: true, status: 200, json: () => pendingSession },
      setInterval() {}, setTimeout, clearTimeout, AbortController,
    };
    vm.runInNewContext(`const basePath="";${script}`, runtime);
    const checking = runtime.checkPermission();
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(elements.refresh.disabled, true);
    const loggingOut = elements.logout.onclick();
    releaseSession(delayedStage === "response" ? Response.json(approved) : approved);
    await checking;
    assert.deepEqual(destinations, [], delayedStage);
    releaseLogout(Response.json({ redirect_to: "/login" }));
    await loggingOut;
    assert.deepEqual(destinations, ["/login"]);
  }
});

async function inlineProfileRuntime(fetch, profileOverrides = {}) {
  const source = await readFile(new URL("../../../src/dcar_eval/dcar_auth/gateway.py", import.meta.url), "utf8");
  const script = source.match(/<script>\nconst basePath=__BASE_PATH_JSON__;([\s\S]*?)<\/script>/)?.[1];
  assert.ok(script);
  const profile = { authenticated: true, username: "new.person@example.com", display_name: "已保存的昵称", role: "new_user", ...profileOverrides };
  const destinations = [];
  function element(initial = {}) {
    const events = new Map();
    const attributes = new Map();
    return {
      hidden: false, disabled: false, value: "", textContent: "", title: "", style: {}, scrollHeight: 32,
      focus() { this.focused = true; },
      setSelectionRange(start, end) {
        this.selectionStart = start;
        this.selectionEnd = end;
        this.selectionAfterFocus = this.focused;
      },
      setAttribute: (name, value) => attributes.set(name, value),
      getAttribute: (name) => attributes.get(name),
      addEventListener: (type, handler) => events.set(type, handler),
      dispatch(type, event = {}) { return (this[`on${type}`] ?? events.get(type))?.(event); },
      ...initial,
    };
  }
  const label = element({ textContent: "刷新权限" });
  const elements = {
    status: element(),
    refresh: element({ querySelector: () => label }),
    logout: element(),
    "profile-data": element({ textContent: JSON.stringify(profile) }),
    "profile-display": element(),
    "profile-nickname": element(),
    "profile-name": element(),
    "profile-role": element(),
    "profile-error": element({ hidden: true }),
    "profile-empty-hint": element({ hidden: true }),
    "profile-save": element(),
    "profile-cancel": element(),
    "profile-form": element({ hidden: true }),
    "open-profile": element(),
  };
  let submitted;
  elements["profile-form"].requestSubmit = () => {
    submitted = elements["profile-form"].onsubmit({ preventDefault() {} });
  };
  const runtime = {
    document: { getElementById: (id) => elements[id], addEventListener() {}, visibilityState: "visible" },
    window: { addEventListener() {} },
    location: { reload: () => destinations.push("reload"), replace: (url) => destinations.push(url) },
    fetch,
    setInterval() {}, setTimeout, clearTimeout, AbortController,
  };
  vm.runInNewContext(`const basePath="";${script}`, runtime);
  return { runtime, elements, profile, destinations, submitted: () => submitted };
}

test("inline nickname prefill is editable and only changed drafts can be saved", async () => {
  for (const displayName of ["", "已保存的昵称 😀"]) {
    const requests = [];
    let savedProfile;
    const state = await inlineProfileRuntime(async (url, options) => {
      requests.push({ url, options });
      if (url.endsWith("/profile")) savedProfile = { ...state.profile, ...JSON.parse(options.body) };
      return Response.json(savedProfile ?? state.profile);
    }, { username: "panyang", display_name: displayName });
    const { elements } = state;
    const nickname = elements["profile-nickname"];
    elements["open-profile"].onclick();
    const initialValue = displayName || "panyang";
    assert.equal(nickname.value, initialValue);
    assert.equal(nickname.placeholder, "输入昵称");
    assert.equal(nickname.disabled, false);
    assert.equal(nickname.selectionAfterFocus, true);
    assert.equal(nickname.selectionStart, initialValue.length);
    assert.equal(nickname.selectionEnd, initialValue.length);
    assert.equal(elements["profile-save"].disabled, true);
    assert.equal(elements["profile-empty-hint"].hidden, true);

    nickname.dispatch("keydown", { key: "Enter", preventDefault() {} });
    await state.submitted();
    await elements["profile-form"].onsubmit({ preventDefault() {} });
    assert.equal(requests.length, 0, "unchanged Enter and direct submit must not write the username as a nickname");

    nickname.value += "-运营";
    nickname.dispatch("input");
    assert.equal(elements["profile-save"].disabled, false);
    nickname.value = initialValue;
    nickname.dispatch("input");
    assert.equal(elements["profile-save"].disabled, true, "reverting the draft disables saving again");
    await elements["profile-form"].onsubmit({ preventDefault() {} });
    assert.equal(requests.length, 0);

    nickname.value += "-运营";
    nickname.dispatch("input");
    nickname.dispatch("keydown", { key: "Enter", preventDefault() {} });
    await state.submitted();
    await new Promise((resolve) => setImmediate(resolve));
    const saves = requests.filter(({ url }) => url.endsWith("/profile"));
    assert.equal(saves.length, 1);
    assert.deepEqual(JSON.parse(saves[0].options.body), { display_name: `${initialValue}-运营` });
    assert.equal(elements["profile-form"].hidden, true);
    assert.equal(elements["profile-name"].textContent, `${initialValue}-运营`);
  }
});

test("editing an inline nickname defers in-flight approval until the edit is cancelled", async () => {
  for (const delayedStage of ["response", "json"]) {
    let releaseSession;
    let requestCount = 0;
    const pendingSession = new Promise((resolve) => { releaseSession = resolve; });
    let approved;
    const { runtime, elements, profile, destinations } = await inlineProfileRuntime(async () => {
      requestCount += 1;
      if (requestCount > 1) return Response.json(approved);
      return delayedStage === "response" ? pendingSession : { ok: true, status: 200, json: () => pendingSession };
    });
    approved = { ...profile, display_name: "服务器中的旧昵称", role: "operator" };
    const checking = runtime.checkPermission();
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(requestCount, 1);
    elements["open-profile"].onclick();
    elements["profile-nickname"].value = "未保存的草稿 @ 😀";
    elements["profile-nickname"].dispatch("input");
    releaseSession(delayedStage === "response" ? Response.json(approved) : approved);
    await checking;

    assert.equal(elements["profile-form"].hidden, false, delayedStage);
    assert.equal(elements["profile-display"].hidden, false, "the identity stays visible while editing");
    assert.equal(elements["open-profile"].hidden, true, "only the edit button is hidden");
    assert.deepEqual(destinations, [], delayedStage);
    assert.equal(elements["profile-nickname"].value, "未保存的草稿 @ 😀", delayedStage);
    assert.equal(elements["profile-name"].textContent, profile.display_name, delayedStage);
    assert.equal(elements["profile-role"].textContent, "待开通权限", delayedStage);
    await runtime.checkPermission();
    await runtime.checkPermission(true);
    assert.equal(requestCount, 1, "automatic and manual refresh remain paused during editing");

    elements["profile-cancel"].onclick();
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(elements["profile-form"].hidden, true, delayedStage);
    assert.equal(elements["profile-display"].hidden, false, delayedStage);
    assert.equal(elements["open-profile"].hidden, false, delayedStage);
    assert.equal(requestCount, 2, delayedStage);
    assert.equal(elements["profile-name"].textContent, approved.display_name, delayedStage);
    assert.deepEqual(destinations, ["reload"], delayedStage);
  }
});

test("inline nickname keyboard shortcuts protect IME input and preserve blurred drafts", async () => {
  const requests = [];
  let savedProfile;
  const state = await inlineProfileRuntime(async (url, options) => {
    requests.push({ url, options });
    if (url.endsWith("/profile")) {
      savedProfile = { ...state.profile, ...JSON.parse(options.body) };
      return Response.json(savedProfile);
    }
    return Response.json(savedProfile ?? state.profile);
  });
  const { runtime, elements } = state;
  const nickname = elements["profile-nickname"];
  elements["open-profile"].onclick();
  nickname.value = "  程鑫 @ / & 😀\n第二行  ";
  nickname.dispatch("input");
  nickname.dispatch("blur");
  assert.equal(elements["profile-form"].hidden, false);
  assert.equal(nickname.value, "  程鑫 @ / & 😀\n第二行  ");
  for (const key of [
    { key: "Enter", shiftKey: true },
    { key: "Enter", isComposing: true },
    { key: "Enter", keyCode: 229 },
    { key: "Escape", isComposing: true },
  ]) {
    let prevented = false;
    nickname.dispatch("keydown", { ...key, preventDefault() { prevented = true; } });
    assert.equal(prevented, false, JSON.stringify(key));
    assert.equal(requests.length, 0, JSON.stringify(key));
    assert.equal(elements["profile-form"].hidden, false, JSON.stringify(key));
  }
  await runtime.checkPermission(true);
  assert.equal(requests.length, 0);
  let prevented = false;
  nickname.dispatch("keydown", { key: "Enter", preventDefault() { prevented = true; } });
  await state.submitted();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(prevented, true);
  assert.equal(requests[0].url, "/auth/profile");
  assert.deepEqual(JSON.parse(requests[0].options.body), { display_name: "  程鑫 @ / & 😀\n第二行  " });
  assert.equal(elements["profile-name"].textContent, nickname.value);
  assert.equal(elements["profile-form"].hidden, true);
  assert.equal(elements["profile-display"].hidden, false);
  assert.equal(elements["open-profile"].hidden, false);
  assert.equal(requests.at(-1).url, "/auth/session", "successful save resumes a fresh permission check");

  elements["open-profile"].onclick();
  nickname.value = "取消这个草稿";
  nickname.dispatch("input");
  nickname.dispatch("keydown", { key: "Escape", preventDefault() {} });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(elements["profile-form"].hidden, true);
  assert.equal(elements["profile-name"].textContent, savedProfile.display_name);
  assert.equal(requests.filter(({ url }) => url.endsWith("/profile")).length, 1);
});

test("failed inline nickname saves retain the draft and permit an empty-name retry", async () => {
  const requests = [];
  let releaseSave;
  const pendingSave = new Promise((resolve) => { releaseSave = resolve; });
  let savedProfile;
  const state = await inlineProfileRuntime(async (url, options) => {
    requests.push({ url, options });
    if (url.endsWith("/profile")) {
      if (requests.length === 1) return pendingSave;
      savedProfile = { ...state.profile, ...JSON.parse(options.body) };
      return Response.json(savedProfile);
    }
    return Response.json(savedProfile ?? state.profile);
  });
  const { runtime, elements } = state;
  elements["open-profile"].onclick();
  elements["profile-nickname"].value = "未保存 @ 😀";
  elements["profile-nickname"].dispatch("input");
  const saving = elements["profile-form"].onsubmit({ preventDefault() {} });
  assert.equal(elements["profile-save"].disabled, true);
  assert.equal(elements["profile-cancel"].disabled, true);
  await runtime.checkPermission();
  await runtime.checkPermission(true);
  assert.equal(requests.length, 1, "refresh stays paused while the save is in flight");
  releaseSave(Response.json({ detail: "暂时无法处理，请稍后重试" }, { status: 503 }));
  await saving;
  assert.equal(elements["profile-form"].hidden, false);
  assert.equal(elements["profile-nickname"].value, "未保存 @ 😀");
  assert.equal(elements["profile-error"].hidden, false);
  assert.equal(elements["profile-save"].disabled, false);
  assert.equal(elements["profile-name"].textContent, state.profile.display_name);

  elements["profile-nickname"].value = "";
  elements["profile-nickname"].dispatch("input");
  assert.equal(elements["profile-empty-hint"].hidden, false);
  assert.equal(elements["profile-save"].disabled, false);
  elements["profile-nickname"].value = "重新输入";
  elements["profile-nickname"].dispatch("input");
  assert.equal(elements["profile-empty-hint"].hidden, true);
  elements["profile-nickname"].value = "";
  elements["profile-nickname"].dispatch("input");
  assert.equal(elements["profile-empty-hint"].hidden, false);
  await elements["profile-form"].onsubmit({ preventDefault() {} });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(JSON.parse(requests[1].options.body), { display_name: "" });
  assert.equal(elements["profile-name"].textContent, state.profile.username);
  assert.equal(elements["profile-form"].hidden, true);
  elements["open-profile"].onclick();
  assert.equal(elements["profile-nickname"].value, state.profile.username);
  assert.equal(elements["profile-empty-hint"].hidden, true);
  assert.equal(elements["profile-save"].disabled, true);
});
