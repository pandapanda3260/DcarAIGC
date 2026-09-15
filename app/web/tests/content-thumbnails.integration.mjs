// DCAR_TEST_DOM_MODULE=/path/to/linkedom node --test tests/content-thumbnails.integration.mjs
// Real React DOM reconciliation and image handlers, without browser automation.
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import React, { act } from "react";
import ts from "typescript";

const require = createRequire(import.meta.url);
assert.ok(process.env.DCAR_TEST_DOM_MODULE, "Set DCAR_TEST_DOM_MODULE to the installed linkedom package");
const { parseHTML } = require(process.env.DCAR_TEST_DOM_MODULE);
const { window } = parseHTML("<!doctype html><html><body></body></html>");
for (const [name, value] of Object.entries({ window, document: window.document, HTMLElement: window.HTMLElement, Node: window.Node, Event: window.Event, navigator: { userAgent: "node" }, IS_REACT_ACT_ENVIRONMENT: true })) {
  Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
}
const { createRoot } = require("react-dom/client");
const app = fileURLToPath(new URL("../app/", import.meta.url));
const modules = new Map();
function load(filename) {
  if (modules.has(filename)) return modules.get(filename).exports;
  const compiled = { exports: {} };
  modules.set(filename, compiled);
  const output = ts.transpileModule(readFileSync(filename, "utf8"), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true } }).outputText;
  function localRequire(name) {
    if (name === "next/image") return function Image(props) { const attributes = { ...props }; delete attributes.unoptimized; return React.createElement("img", attributes); };
    if (name.startsWith("@phosphor")) return new Proxy({}, { get: () => () => React.createElement("svg") });
    if (!name.startsWith(".")) return require(name);
    const resolved = path.resolve(path.dirname(filename), name);
    return load([resolved + ".tsx", resolved + ".ts", resolved].find(existsSync));
  }
  new Function("require", "module", "exports", output)(localRequire, compiled, compiled.exports);
  return compiled.exports;
}
const Box = load(path.join(app, "contents/ContentMediaBox.tsx")).default;
const first = "https://first.example/cover.webp?signature=a%2Fb";
const second = "https://second.example/cover.webp";
const third = "https://third.example/cover.jpg";
const base = { id: 1, title: "封面测试", platform: "kuaishou", local_media_available: false, content_type: "video", canonical_url: "https://www.kuaishou.com/short-video/real-post" };
const imageProps = (image) => image[Object.keys(image).find((key) => key.startsWith("__reactProps$"))];

function setup(t) {
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  const opened = [];
  t.after(async () => { await act(async () => root.unmount()); container.remove(); });
  return {
    container, opened,
    image: () => container.querySelector("img.content-thumbnail"),
    box: () => container.querySelector(".content-media-box"),
    async render(thumbnail, item = base) {
      await act(async () => root.render(React.createElement(React.StrictMode, null, React.createElement(Box, { item, thumbnail, onOpen: (selected) => opened.push(selected), showPlatformMark: false }))));
    },
    async dispatch(image, type) { await act(async () => image.dispatchEvent(new window.Event(type))); },
  };
}

test("the actual component advances failed sources once, ignores stale events, and marks only the current image loaded", async (t) => {
  const view = setup(t);
  await view.render({ remote_url: first, remote_urls: [first, second, third] });
  const oldImage = view.image();
  const stale = imageProps(oldImage);
  assert.equal(oldImage.getAttribute("src"), first);
  assert.equal(oldImage.getAttribute("loading"), "lazy");
  assert.equal(oldImage.getAttribute("referrerPolicy"), "no-referrer");
  await act(async () => { stale.onError(); stale.onError(); stale.onLoad(); });
  assert.equal(view.image().getAttribute("src"), second, "duplicate failure must not skip the second source");
  assert.equal(view.image().getAttribute("data-loaded"), "false", "old load must not mark the replacement loaded");
  assert.notEqual(view.image(), oldImage, "each source gets a fresh image element");
  await view.dispatch(view.image(), "load");
  assert.equal(view.image().getAttribute("data-loaded"), "true");
  await act(async () => { stale.onError(); stale.onLoad(); });
  assert.equal(view.image().getAttribute("src"), second);
  assert.equal(view.image().getAttribute("data-loaded"), "true");
  assert.doesNotMatch(view.box().title, /封面加载失败/);
  assert.equal(view.box().getAttribute("href"), base.canonical_url);
  assert.equal(view.box().getAttribute("target"), "_blank");
});

test("all three failures stop permanently until the effective candidate list or item changes", async (t) => {
  const view = setup(t);
  const thumbnail = { remote_url: first, remote_urls: [first, first, second, third, "https://fourth.example/x"] };
  await view.render(thumbnail);
  const oldHandlers = [];
  for (const expected of [first, second, third]) {
    assert.equal(view.image().getAttribute("src"), expected);
    oldHandlers.push(imageProps(view.image()));
    await view.dispatch(view.image(), "error");
  }
  assert.equal(view.image(), null);
  assert.match(view.box().title, /封面加载失败/);
  await view.render({ ...thumbnail, remote_urls: [...thumbnail.remote_urls] });
  await act(async () => oldHandlers.forEach((handler) => { handler.onError(); handler.onLoad(); }));
  assert.equal(view.image(), null, "the same values and late events cannot restart exhausted attempts");
  assert.match(view.box().title, /封面加载失败/);

  await view.render({ remote_url: first, remote_urls: [first, third, second] });
  assert.equal(view.image().getAttribute("src"), first, "fallback-only changes restart the whole list");
  assert.equal(view.image().getAttribute("data-loaded"), "false");
  await act(async () => oldHandlers.forEach((handler) => { handler.onError(); handler.onLoad(); }));
  assert.equal(view.image().getAttribute("src"), first, "events from the old list cannot affect the reset component");
  assert.equal(view.image().getAttribute("data-loaded"), "false");
  await view.dispatch(view.image(), "load");
  assert.equal(view.image().getAttribute("data-loaded"), "true");
  await view.render({ remote_url: first, remote_urls: [first, third, second] }, { ...base, id: 2 });
  assert.equal(view.image().getAttribute("src"), first);
  assert.equal(view.image().getAttribute("data-loaded"), "false", "another item starts its own load state");
});

test("legacy metadata works and failure hints preserve the media button and its click action", async (t) => {
  const view = setup(t);
  const item = { ...base, local_media_available: true };
  await view.render({ remote_url: second }, item);
  assert.equal(view.image().getAttribute("src"), second);
  await view.dispatch(view.image(), "error");
  assert.equal(view.image(), null);
  assert.match(view.box().title, /封面加载失败/);
  await act(async () => view.box().dispatchEvent(new window.Event("click", { bubbles: true })));
  assert.deepEqual(view.opened, [item]);
  for (const [reason, hint] of [["not_found", "未取得封面"], ["unsupported_format", "封面格式暂不支持"], ["source_unavailable", "封面资料暂不可用"]]) {
    await view.render({ remote_url: null, remote_urls: [], reason }, item);
    assert.equal(view.image(), null);
    assert.equal(view.box().title, `播放；${hint}`);
  }
  await view.render(undefined, item);
  assert.equal(view.box().title, "播放", "pending metadata must not be reported as missing");
  await view.render({ remote_url: second, reason: "unsupported_format" }, item);
  assert.equal(view.box().title, "播放", "an available image takes precedence over a stale reason");
});
