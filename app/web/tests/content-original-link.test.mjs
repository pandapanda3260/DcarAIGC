import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";
import { originalPostUrl, resolveMediaAction } from "../app/lib/contentMedia.ts";

const require = createRequire(import.meta.url);
const app = fileURLToPath(new URL("../app/", import.meta.url));
const platforms = ["douyin", "xiaohongshu", "kuaishou", "wechat_channels"];
const badUrls = ["", " ", null, undefined, "/contents", "//example.com/post", "javascript:alert(1)", "data:text/html,hello", "https://", "http:example.com", "https://user:password@example.com/post", "https://example.com\\post", "https://example.com/\npost", " https://example.com/post", "https://example.com:99999/post"];
const base = { id: 82120, title: "实际作品标题", platform_content_id: "real-provider-id", canonical_url: "", local_media_available: false, content_type: "video", link_id: "C82120", raw_account_name: "账号", raw_account_uid: "author", published_at: "2026-09-13T00:00:00Z", metrics_captured_at: null, body: "正文", metrics: {}, spu: null };

// Render the actual TSX with React SSR. Only routing, network, and dialog focus
// are isolated; no production service or provider is contacted by this suite.
function load(relative, { bundle = null } = {}) {
  const cache = new Map();
  function module(filename) {
    if (cache.has(filename)) return cache.get(filename).exports;
    let source = readFileSync(filename, "utf8");
    if (filename.endsWith("ContentsPage.tsx")) source = source.replace("function ContentDetails(", "export function ContentDetails(");
    const compiled = { exports: {} }; cache.set(filename, compiled);
    const output = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true } }).outputText;
    let stateIndex = 0;
    function localRequire(name) {
      if (name === "react" && filename.endsWith("EvidenceModal.tsx")) return { ...React, useState: (initial) => React.useState(stateIndex++ === 0 ? bundle : initial) };
      if (name === "react" || name === "react/jsx-runtime") return require(name);
      if (name === "next/image") return function Image(props) {
        const attributes = { ...props };
        delete attributes.unoptimized;
        return React.createElement("img", attributes);
      };
      if (name === "next/navigation") return {};
      if (name.startsWith("@phosphor")) return new Proxy({}, { get: () => () => React.createElement("svg") });
      const resolved = path.resolve(path.dirname(filename), name);
      if (resolved.endsWith(".module.css")) return new Proxy({}, { get: (_, key) => key });
      if (resolved === path.join(app, "lib/api")) return { apiUrl: (url) => url, readQueryJson: () => { throw new Error("Network forbidden"); } };
      if (resolved === path.join(app, "lib/useDialogFocus")) return { useDialogFocus() {} };
      if (resolved === path.join(app, "components/ContentUpdateJobsProvider")) return { useContentUpdateJobs: () => ({ available: false, locked: () => false }) };
      if (resolved === path.join(app, "contents/ContentDialog")) return function ContentDialog({ title, subtitle, children, footer }) { return React.createElement("section", {}, title, subtitle, children, footer); };
      if (filename.endsWith("ContentsPage.tsx") && name !== "../lib/format" && name !== "../lib/contentMedia" && name !== "../lib/accountClassification" && name !== "../lib/duplicateStatus" && name !== "./ContentMediaBox") return {};
      if (!name.startsWith(".")) return require(name);
      const target = [resolved + ".tsx", resolved + ".ts", resolved].find(existsSync);
      return module(target);
    }
    new Function("require", "module", "exports", output)(localRequire, compiled, compiled.exports);
    return compiled.exports;
  }
  return module(path.join(app, relative));
}
const html = (component, props) => renderToStaticMarkup(React.createElement(component, props));

test("original post URL requires an absolute HTTP(S) address and preserves valid signed queries", () => {
  for (const value of badUrls) assert.equal(originalPostUrl(value), null, String(value));
  for (const value of ["https://example.com/post?token=a%2Fb&x=1#part", "http://example.com/post", "HTTPS://example.com/post"]) assert.equal(originalPostUrl(value), value);
});

test("four platforms without a URL or saved media render static covers and non-link titles", () => {
  const Box = load("contents/ContentMediaBox.tsx").default;
  const Title = load("contents/ContentTitle.tsx").default;
  for (const platform of platforms) for (const canonical_url of badUrls) {
    const item = { ...base, platform, canonical_url };
    const cover = html(Box, { item, onOpen: () => assert.fail("must not open"), showPlatformMark: false });
    assert.match(cover, /原帖链接暂不可用/);
    assert.match(cover, /data-action="unavailable"/);
    assert.doesNotMatch(cover, /<a\b|<button\b|播放|content-media-glyph|href=/);
    const title = html(Title, { text: item.title, href: canonical_url });
    assert.match(title, /实际作品标题/); assert.doesNotMatch(title, /<a\b|href=/);
  }
});

test("saved media and the official player remain available independently of original-post URLs", () => {
  const Box = load("contents/ContentMediaBox.tsx").default;
  for (const platform of platforms) {
    const item = { ...base, platform, local_media_available: true };
    assert.equal(resolveMediaAction(item).kind, "local");
    const cover = html(Box, { item, onOpen() {} });
    assert.match(cover, /<button\b/); assert.match(cover, /播放/); assert.doesNotMatch(cover, /<a\b/);
    for (const flag of [undefined, null, false]) assert.equal(resolveMediaAction({ ...item, local_media_available: flag }).kind, "unavailable");
  }
  assert.equal(resolveMediaAction({ ...base, platform: "douyin", platform_content_id: "7681690664354041114" }).kind, "douyin_player");
});

test("valid original links retain anchors and existing media routing", () => {
  const Box = load("contents/ContentMediaBox.tsx").default;
  const Title = load("contents/ContentTitle.tsx").default;
  for (const platform of platforms) {
    const item = { ...base, platform, canonical_url: "https://example.com/post?token=a%2Fb" };
    assert.equal(resolveMediaAction(item).kind, "original");
    for (const rendered of [html(Box, { item, onOpen() {} }), html(Title, { text: item.title, href: item.canonical_url })]) {
      assert.match(rendered, /href="https:\/\/example.com\/post\?token=a%2Fb"/); assert.match(rendered, /target="_blank"/);
    }
    assert.equal(resolveMediaAction({ ...item, local_media_available: undefined }).kind, "local");
  }
});

test("content details retain the actual platform ID and evidence actions without fake original links", () => {
  const Details = load("contents/ContentsPage.tsx").ContentDetails;
  for (const platform of platforms) for (const canonical_url of ["", "javascript:alert(1)", "https://example.com/post"]) {
    const rendered = html(Details, { item: { ...base, platform, canonical_url }, status: { error: "", message: "" } });
    assert.match(rendered, /real-provider-id/); assert.match(rendered, /查看依据/);
    if (originalPostUrl(canonical_url)) assert.match(rendered, /href="https:\/\/example.com\/post"/);
    else { assert.match(rendered, /原帖链接暂不可用/); assert.doesNotMatch(rendered, /<a\b|打开原帖|href=""/); }
  }
});

test("media and evidence modals retain internal resources but omit unavailable original-post exits", () => {
  const Modal = load("contents/ContentMediaModal.tsx").default;
  const item = { ...base, platform: "wechat_channels" };
  for (const available of [false, true]) {
    const rendered = html(Modal, { item: { ...item, local_media_available: available }, action: resolveMediaAction({ ...item, local_media_available: available }), onClose() {} });
    assert.match(rendered, /原帖链接暂不可用/); assert.doesNotMatch(rendered, /<a\b|打开原帖|href=""/);
    if (available) assert.match(rendered, /正在读取已保存的资料/);
  }
  const bundle = { content: item, media: [], media_availability: { status: "unavailable", reason: "暂无本地媒体" }, keyframes: [], evidence: {}, media_lifecycle: null, media_sources: [], asr: {status:"missing",text:""}, ocr: {status:"missing",text:"",observation_count:0}, comments: {stored_count:0,top_items:[]}, processing_slots: [], processing_attempts: [], evaluation: null, display_evaluation_id: null };
  const Evidence = load("contents/EvidenceModal.tsx", { bundle }).default;
  const rendered = html(Evidence, { item, onClose() {}, onChanged() {}, onFeedback() {}, returnToDetails: true });
  assert.match(rendered, /原帖链接暂不可用/); assert.match(rendered, /返回详情/);
  assert.doesNotMatch(rendered, /href=""|打开原链接/);
});
