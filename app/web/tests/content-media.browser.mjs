import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import http from "node:http";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

// Actual React components in Chromium, with temporary media and mocked network.
// Run separately from source/unit tests: npm run test:media-browser.
const require = createRequire(import.meta.url);
const { build } = require("esbuild");
const { chromium } = require("playwright");
const root = fileURLToPath(new URL("../", import.meta.url));
const item = { id: 1, link_id: "TEST", title: "媒体测试", platform: "douyin",
  content_type: "video", platform_content_id: "7681690664354041114",
  canonical_url: "https://www.douyin.com/video/7681690664354041114",
  raw_account_name: "样本", published_at: "2026-09-05T01:00:00Z", local_media_available: true };
const original = { artifact_id: 1, index: 0, kind: "video", name: "source.mp4",
  url: "/original.mp4", bundle_id: "bundle" };
const preview = { artifact_id: 2, index: 0, kind: "image", name: "preview.png",
  url: "/preview.png", bundle_id: "bundle", original_index: 0 };
const hot = { content: item, media: [original], previews: [preview],
  media_availability: { status: "available", reason: "" }, read_only: false,
  media_lifecycle: { bundle_id: "bundle", state: "hot", operation_state: "idle",
    http_status: 200, reason: "original_available", read_only: false } };
const png = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aP1cAAAAASUVORK5CYII=", "base64");

test("media playback and cross-origin focus regressions", async (t) => {
  const temporary = await mkdtemp(path.join(tmpdir(), "dcar-media-browser-"));
  t.after(() => rm(temporary, { recursive: true, force: true }));
  // VP9/WebM：Playwright 自带的 Chromium 不含 H.264 解码器，libx264 样本在它上面永远到不了 readyState 2。
  const videoPath = path.join(temporary, "sample.webm");
  execFileSync("ffmpeg", ["-nostdin", "-loglevel", "error", "-f", "lavfi", "-i",
    "color=c=blue:s=160x90:d=2", "-c:v", "libvpx-vp9", "-pix_fmt", "yuv420p", videoPath]);
  const video = await readFile(videoPath);
  const compiled = await build({ absWorkingDir: root, bundle: true, write: false,
    format: "iife", platform: "browser", jsx: "automatic",
    define: { "process.env.NODE_ENV": '"production"', "process.env.NEXT_PUBLIC_DCAR_API_BASE": '""',
      "process.env.NEXT_PUBLIC_DCAR_BASE_PATH": '""' },
    stdin: { resolveDir: root, loader: "tsx", contents: `
      import React, {useState} from "react";
      import {createRoot} from "react-dom/client";
      import Modal from "./app/contents/ContentMediaModal";
      import {resolveMediaAction} from "./app/lib/contentMedia";
      function App() {
        const [open,setOpen]=useState(false);
        const item=window.reviewItem;
        return <><button id="trigger" onClick={()=>setOpen(true)}>打开媒体</button>
          <button id="background">背景操作</button>
          {open&&<Modal item={item} action={resolveMediaAction(item)} onClose={()=>setOpen(false)}/>}</>;
      }
      createRoot(document.getElementById("root")).render(<App/>);` },
  });
  const styles = (await readFile(path.join(root, "app/globals.css"), "utf8")).replace(/^@import[^;]+;/, "");
  let fixture = hot;
  let useLocal = true;
  const requests = [];
  const server = http.createServer((req, res) => {
    requests.push({ url: req.url, method: req.method });
    if (req.url === "/bundle.js") { res.setHeader("Content-Type", "application/javascript"); res.end(compiled.outputFiles[0].text); }
    else if (req.url === "/style.css") { res.setHeader("Content-Type", "text/css"); res.end(styles); }
    else if (req.url.includes("/evidence")) { res.setHeader("Content-Type", "application/json"); res.end(JSON.stringify(fixture)); }
    else if (req.url === "/preview.png") { res.setHeader("Content-Type", "image/png"); res.end(png); }
    else if (req.url === "/original.mp4") { res.setHeader("Content-Type", "video/webm"); res.end(video); }
    else { res.setHeader("Content-Type", "text/html; charset=utf-8"); res.end('<html><head><link rel="stylesheet" href="/style.css"></head><body><div id="root"></div><script>window.reviewItem=' + JSON.stringify({ ...item, local_media_available: useLocal }) + '</script><script src="/bundle.js"></script></body></html>'); }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const browser = await chromium.launch({ headless: true,
    ...(process.env.DCAR_TEST_CHROMIUM_EXECUTABLE ? { executablePath: process.env.DCAR_TEST_CHROMIUM_EXECUTABLE } : {}) });
  t.after(() => browser.close());
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const base = `http://127.0.0.1:${server.address().port}`;
  await page.route("**/*", (route) => {
    if (route.request().url().startsWith(base)) return route.continue();
    if (route.request().url().startsWith("https://open.douyin.com/player/video?")) {
      return route.fulfill({ contentType: "text/html; charset=utf-8",
        body: '<button id="play">播放</button><button id="volume">声音</button>' });
    }
    throw new Error(`Unexpected network: ${route.request().url()}`);
  });

  await t.test("hot video plays its original and closing releases it", async () => {
    await page.goto(base);
    await page.locator("#trigger").waitFor();
    assert.equal(requests.filter((r) => /evidence|mp4|preview/.test(r.url)).length, 0);
    await page.locator("#trigger").click();
    await page.waitForFunction(() => document.querySelector("video")?.readyState >= 2);
    await page.waitForFunction(() => {
      const video = document.querySelector("video");
      return video && !video.paused && video.currentTime > 0;
    });
    assert.equal(await page.locator(".content-media-image").count(), 0);
    assert.equal(await page.locator("video").getAttribute("src"), "/original.mp4");
    assert.equal(await page.locator(".content-media-toolbar").count(), 0);
    // 面板宽度跟内容走：loadedmetadata 后把 160×90 的宽高比写进 --media-ratio。
    await page.waitForFunction(() => document.querySelector(".content-media-modal")?.style.getPropertyValue("--media-ratio") === "1.7778");
    assert.equal(requests.filter((r) => r.url.includes("/evidence")).length, 1);
    assert.equal(requests.filter((r) => r.url === "/preview.png").length, 0);
    await page.evaluate(() => { window.savedVideo = document.querySelector("video"); });
    await page.locator(".modal-close").click();
    await page.waitForFunction(() => document.activeElement?.id === "trigger");
    assert.deepEqual(await page.evaluate(() => ({ src: window.savedVideo.getAttribute("src"),
      paused: window.savedVideo.paused, connected: window.savedVideo.isConnected })),
    { src: null, paused: true, connected: false });
    assert.equal(await page.locator("#background").evaluate((el) => el.inert), false);
  });

  await t.test("Tab exits native video controls to the parent-owned close button", async () => {
    await page.goto(base);
    await page.locator("#trigger").click();
    await page.waitForFunction(() => document.querySelector("video")?.readyState >= 2);
    await page.locator("video").hover();
    const session = await page.context().newCDPSession(page);
    try {
      const { nodes } = await session.send("Accessibility.getFullAXTree");
      const timeline = nodes.find((node) => node.role?.value === "slider"
        && /time|时间|进度/i.test(node.name?.value ?? ""));
      assert.ok(timeline?.backendDOMNodeId, "Chromium exposes its native video timeline");
      const { model } = await session.send("DOM.getBoxModel", { backendNodeId: timeline.backendDOMNodeId });
      await page.mouse.click((model.content[0] + model.content[2]) / 2,
        (model.content[1] + model.content[5]) / 2);
      assert.equal(await page.locator("video").evaluate((el) => el === document.activeElement), true);
      // Chromium's native media shadow controls keep both keydown and keyup
      // inside the control, including capture listeners on window/document.
      // Do not promise Escape there: a keyboard-reachable parent close button
      // must remain available. Allow a future browser to forward Escape too.
      await page.keyboard.press("Escape");
      if (await page.locator('[role="dialog"]').count()) {
        let reachedClose = false;
        for (let tabs = 0; tabs < 12; tabs += 1) {
          await page.keyboard.press("Tab");
          assert.notEqual(await page.evaluate(() => document.activeElement?.id), "background");
          reachedClose = await page.locator(".modal-close").evaluate((el) => el === document.activeElement);
          if (reachedClose) break;
        }
        assert.equal(reachedClose, true, "Tab exits native controls to the close button");
        await page.keyboard.press("Escape");
      }
      await page.waitForFunction(() => !document.querySelector('[role="dialog"]'));
      await page.waitForFunction(() => document.activeElement?.id === "trigger");
      assert.equal(await page.locator("#background").evaluate((el) => el.inert), false);
    } finally {
      await session.detach();
    }
  });

  await t.test("archived and replica media only read retained previews", async () => {
    for (const readOnly of [false, true]) {
      fixture = { ...hot, read_only: readOnly, media_lifecycle: { ...hot.media_lifecycle,
        read_only: readOnly, state: readOnly ? "hot" : "archived", http_status: 409,
        reason: readOnly ? "replica_original_omitted" : "original_archived" } };
      requests.length = 0;
      await page.goto(base);
      await page.locator("#trigger").click();
      await page.locator(".content-media-image").waitFor();
      assert.equal(await page.locator("video").count(), 0);
      assert.equal(await page.locator(".content-media-image").getAttribute("src"), "/preview.png");
      assert.equal(requests.filter((r) => r.url === "/original.mp4").length, 0);
      await page.waitForFunction(() => document.querySelector(".content-media-modal")?.style.getPropertyValue("--media-ratio") === "1.0000");
      assert.match(await page.locator(".content-media-toolbar").textContent(), /原件已归档或不可用，显示保留预览/);
      await page.keyboard.press("Escape");
      await page.waitForFunction(() => !document.querySelector('[role="dialog"]'));
    }
  });

  await t.test("an older API without availability flags still plays saved media on click", async () => {
    useLocal = undefined;
    fixture = hot;
    requests.length = 0;
    await page.goto(base);
    await page.locator("#trigger").waitFor();
    assert.equal(requests.filter((r) => /evidence|mp4|preview/.test(r.url)).length, 0);
    await page.locator("#trigger").click();
    await page.waitForFunction(() => document.querySelector("video")?.readyState >= 2);
    assert.equal(await page.locator("iframe").count(), 0);
    assert.equal(requests.filter((r) => r.url.includes("/evidence")).length, 1);
    await page.locator(".modal-close").click();
    await page.waitForFunction(() => document.activeElement?.id === "trigger");
  });

  await t.test("Tab reaches the iframe, exits to close, and cannot reach the background", async () => {
    useLocal = false;
    requests.length = 0;
    await page.goto(base);
    await page.locator("#trigger").click();
    const frame = page.frameLocator("iframe");
    await frame.locator("#play").waitFor();
    await page.waitForFunction(() => document.activeElement?.classList.contains("modal-close"));
    assert.equal(await page.locator("#background").evaluate((el) => el.inert), true);
    // × 是最后一个可聚焦元素：Tab 由焦点圈定回绕到第一个（iframe 本身），再 Tab 进入播放器内部。
    await page.keyboard.press("Tab");
    assert.equal(await page.evaluate(() => document.activeElement?.tagName), "IFRAME");
    await page.keyboard.press("Tab");
    assert.equal(await frame.locator("#play").evaluate((el) => el === document.activeElement), true);
    // Cross-origin key events stay in the child document. The following Tab
    // path must provide a parent-owned close control even when Escape cannot.
    await page.keyboard.press("Escape");
    assert.equal(await page.locator('[role="dialog"]').count(), 1);
    await page.keyboard.press("Tab");
    assert.equal(await frame.locator("#volume").evaluate((el) => el === document.activeElement), true);
    // 从播放器 Tab 出来先落到父文档自己的控件：打开原帖，再到关闭。
    await page.keyboard.press("Tab");
    assert.equal(await page.locator('.content-media-player-help a').evaluate((el) => el === document.activeElement), true);
    assert.equal(await page.locator('.content-media-player-help').isVisible(), true);
    assert.equal(await page.locator('.content-media-player-help a').getAttribute("href"), item.canonical_url);
    for (const viewport of [{ width: 1280, height: 900 }, { width: 1512, height: 711 }]) {
      await page.setViewportSize(viewport);
      const layout = await page.evaluate(() => {
        const stage = document.querySelector('.content-media-stage').getBoundingClientRect();
        const help = document.querySelector('.content-media-player-help').getBoundingClientRect();
        const panel = document.querySelector('.content-media-modal').getBoundingClientRect();
        return { stageBottom: stage.bottom, helpTop: help.top, helpBottom: help.bottom, panelBottom: panel.bottom };
      });
      assert.ok(layout.helpTop >= layout.stageBottom, 'fallback remains below the player');
      assert.ok(layout.helpBottom <= layout.panelBottom, 'fallback remains inside the dialog');
    }
    await page.keyboard.press("Tab");
    assert.equal(await page.locator('.content-media-actions a').evaluate((el) => el === document.activeElement), true);
    await page.keyboard.press("Tab");
    assert.equal(await page.locator(".modal-close").evaluate((el) => el === document.activeElement), true);
    await page.keyboard.press("Shift+Tab");
    assert.equal(await page.locator('.content-media-actions a').evaluate((el) => el === document.activeElement), true);
    await page.keyboard.press("Shift+Tab");
    assert.equal(await page.locator('.content-media-player-help a').evaluate((el) => el === document.activeElement), true);
    await page.keyboard.press("Shift+Tab");
    assert.equal(await frame.locator("#volume").evaluate((el) => el === document.activeElement), true);
    await page.keyboard.press("Tab");
    await page.keyboard.press("Escape");
    await page.waitForFunction(() => document.activeElement?.id === "trigger");
    assert.equal(await page.locator("iframe").count(), 0);
    assert.equal(await page.locator("#background").evaluate((el) => el.inert), false);
    assert.equal(requests.filter((r) => /evidence|mp4|preview/.test(r.url)).length, 0);
  });
  assert.deepEqual(errors, []);
  assert.ok(requests.every((r) => r.method === "GET"));
});
