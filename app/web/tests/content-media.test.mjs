import assert from "node:assert/strict";
import test from "node:test";
import {
  DOUYIN_ITEM_ID_PATTERN, PLATFORM_LOGO_PATHS, douyinPlayerEligible, douyinPlayerUrl, mediaLabelFor, originalPostAction, originalPostHint, resolveMediaAction,
} from "../app/lib/contentMedia.ts";

function item(overrides = {}) {
  return {
    platform: "douyin", content_type: "video", platform_content_id: "7681690664354041114",
    canonical_url: "https://www.douyin.com/video/7681690664354041114", local_media_available: false, ...overrides,
  };
}

test("douyin player url is only built from a numeric item id", () => {
  assert.equal(douyinPlayerUrl("7681690664354041114"), "https://open.douyin.com/player/video?vid=7681690664354041114&autoplay=0&mode=mobile&width=100vw&height=100vh");
  for (const bad of [null, undefined, "", "12a", "1234", "1".repeat(26), " 7681690664354041114", "69e36755000000001f007b15", "https://x"]) {
    assert.equal(douyinPlayerUrl(bad), null, String(bad));
  }
  assert.ok(DOUYIN_ITEM_ID_PATTERN.test("12345"));
});

test("local media wins over every other tier and keeps the content-type label", () => {
  assert.deepEqual(resolveMediaAction(item({ local_media_available: true })), { kind: "local", label: "播放", href: null, playerUrl: null });
  assert.equal(resolveMediaAction(item({ local_media_available: true, content_type: "image" })).label, "查看");
  assert.equal(resolveMediaAction(item({ local_media_available: true, platform: "xiaohongshu", content_type: "normal" })).label, "查看");
  assert.equal(resolveMediaAction(item({ local_media_available: true, platform_content_id: "bad-id" })).kind, "local");
});

test("douyin video or unknown without local media goes to the official player", () => {
  const video = resolveMediaAction(item());
  assert.deepEqual(video, { kind: "douyin_player", label: "播放", href: null, playerUrl: "https://open.douyin.com/player/video?vid=7681690664354041114&autoplay=0&mode=mobile&width=100vw&height=100vh" });
  assert.equal(resolveMediaAction(item({ content_type: "unknown" })).kind, "douyin_player");
  assert.equal(resolveMediaAction(item({ content_type: "unknown" })).label, "播放");
  assert.ok(douyinPlayerEligible(item()));
});

test("older APIs do not hide saved media when the availability field is missing", () => {
  for (const flag of [undefined, null]) {
    for (const platform of ["douyin", "xiaohongshu", "wechat_channels", "kuaishou"]) {
      assert.equal(resolveMediaAction(item({ platform, local_media_available: flag })).kind, "local");
    }
  }
});

test("everything else opens the original post in a new tab", () => {
  const xhs = resolveMediaAction(item({ platform: "xiaohongshu", platform_content_id: "69e36755000000001f007b15", canonical_url: "https://www.xiaohongshu.com/explore/69e36755000000001f007b15" }));
  assert.deepEqual(xhs, { kind: "original", label: "播放", href: "https://www.xiaohongshu.com/explore/69e36755000000001f007b15", playerUrl: null });
  assert.equal(resolveMediaAction(item({ platform: "xiaohongshu", content_type: "normal" })).label, "查看");
  assert.equal(resolveMediaAction(item({ content_type: "image" })).kind, "original");
  assert.equal(resolveMediaAction(item({ platform_content_id: null })).kind, "original");
  assert.equal(resolveMediaAction(item({ platform_content_id: "7681690664354041114abc" })).kind, "original");
  assert.equal(resolveMediaAction(item({ platform: "kuaishou" })).kind, "original");
});

test("the original-post box explains itself in Chinese with the platform name", () => {
  // 角标只是一个外链箭头图标，含义靠这两句：无障碍名称用短句，悬停提示说明为什么要跳出去。
  // 入参是 format.label 换好的平台中文名（ContentMediaBox 负责换），本模块不引其它 lib。
  assert.equal(originalPostAction("抖音"), "去抖音查看原作品");
  assert.equal(originalPostAction("小红书"), "去小红书查看原作品");
  assert.equal(originalPostHint("视频号"), "站内未保存这条内容的视频或图片，点击去视频号查看原作品");
  for (const text of [originalPostAction("抖音"), originalPostHint("抖音")]) assert.doesNotMatch(text, /原帖|[A-Za-z]/);
});

test("labels and logos stay in sync with the platform keys", () => {
  assert.equal(mediaLabelFor("video"), "播放");
  for (const other of ["image", "normal", "unknown", null, undefined]) assert.equal(mediaLabelFor(other), "查看");
  assert.deepEqual(Object.keys(PLATFORM_LOGO_PATHS).sort(), ["douyin", "kuaishou", "wechat_channels", "xiaohongshu"]);
  for (const path of Object.values(PLATFORM_LOGO_PATHS)) assert.match(path, /^\/brand-[a-z-]+-official\.png$/);
});
