#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const collector = require("../src/dcar_eval/xhs_browser_comment_collector.js");

assert.equal(collector.VERSION, "xhs-browser-dom-v1");
assert.equal(collector.normalizeText("  怎么\n  预定？  "), "怎么 预定？");
assert.equal(
  collector.parseNoteId("/explore/6a38c5eb0000000020038e67"),
  "6a38c5eb0000000020038e67",
);
assert.equal(
  collector.parseNoteId("/discovery/item/6A38C5EB0000000020038E67"),
  "6a38c5eb0000000020038e67",
);
assert.equal(collector.parseNoteId("/search_result/anything"), null);
assert.equal(collector.parsePlatformCommentCount("共 242 条评论"), 242);
assert.equal(collector.parsePlatformCommentCount("共 1,234 条评论"), 1234);
assert.equal(collector.parsePlatformCommentCount("评论暂不可见"), null);
assert.equal(
  collector.publicSourceUrl(
    "https://www.xiaohongshu.com/explore/abc?xsec_token=secret&xsec_source=app_share",
  ),
  "https://www.xiaohongshu.com/explore/abc",
);
assert.equal(collector.publicSourceUrl("not a url"), null);

console.log("xhs_browser_comment_collector: 10 assertions passed");
