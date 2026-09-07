import assert from "node:assert/strict";
import test from "node:test";
import { contentUpdateFeedback } from "../app/contents/contentUpdate.ts";

const result = (overrides = {}) => ({
  status: "succeeded", provider_cost: 0, currency: "USD",
  stages: [{ stage: "comments", status: "already_succeeded" }],
  metrics: { status: "succeeded", missing_fields: [] },
  media: { status: "evidence_ready" }, ...overrides,
});

test("completed update names the content and correctly reports cached zero-cost work", () => {
  assert.deepEqual(contentUpdateFeedback(result(), "CGCES6"), {
    error: "", message: "CGCES6 的数据已更新；本次未产生付费服务费用。",
  });
  const replay = contentUpdateFeedback(result({ stages: [{ stage: "detail", status: "replayed" }] }));
  assert.match(replay.message, /^数据已更新/);
});

test("partial counters identify missing fields without inventing successful acquisition", () => {
  const value = contentUpdateFeedback(result({
    status: "partial", provider_cost: 0.001,
    metrics: { status: "partial", missing_fields: ["view_count", "like_count", "collect_count", "view_count"] },
  }));
  assert.equal(value.error, "");
  assert.match(value.message, /数据更新未全部完成；阅读数、点赞数、收藏数尚无新数据/);
  assert.match(value.message, /\$0\.001/);
  assert.doesNotMatch(value.message, /只更新了一部分|已更新|undefined/);
});

test("all failed stages are not described as a partial success", () => {
  const value = contentUpdateFeedback(result({
    status: "partial", stages: [
      { stage: "detail", status: "failed", error_code: "forward_build_invalid" },
      { stage: "comments", status: "failed" },
    ],
  }));
  assert.match(value.message, /内容资料、评论资料更新未完成/);
  assert.doesNotMatch(value.message, /已更新|部分成功|forward_build_invalid/);
});

test("media limitations remain visible even when backend aggregate says succeeded", () => {
  for (const [media, expected] of [
    [{ status: "restore_required", can_restore: true }, /请在查看依据中申请恢复/],
    [{ status: "restore_required", can_restore: false }, /请在查看依据中查看状态/],
    [{ status: "restore_required", reason: "original_restoring" }, /正在恢复/],
    [{ status: "expired_non_replayable" }, /已到保留期限，无法重新处理/],
    [{ status: "retryable_failed" }, /媒体处理失败/],
    [{ status: "no_source" }, /尚无可处理的媒体资料/],
    [{ status: "legacy_source_skipped" }, /已有媒体未重新处理/],
  ]) {
    const value = contentUpdateFeedback(result({ media }));
    assert.equal(value.error, "");
    assert.match(value.message, /更新未全部完成/);
    assert.match(value.message, expected);
  }
});

test("failed and unexpected top-level statuses never imply partial success or a queued job", () => {
  for (const status of ["failed", "queued", "running", "unknown", "", null, undefined]) {
    const value = contentUpdateFeedback(result({ status, provider_cost: 0.001 }));
    assert.notEqual(value.error, "");
    assert.equal(value.message, "");
    assert.match(value.error, /\$0\.001/);
    assert.doesNotMatch(value.error, /部分成功|已更新|已排队|只更新了一部分/);
  }
});

test("unrecognized nested status cannot be promoted to a complete or partial success", () => {
  for (const override of [
    { media: { status: "unrecognized_failure" } },
    { metrics: { status: "failed" } },
    { stages: [{ stage: "detail", status: "running" }] },
  ]) {
    const value = contentUpdateFeedback(result(override));
    assert.match(value.error, /未能确认/);
    assert.equal(value.message, "");
  }
});

test("missing and invalid costs are not silently treated as free or displayed as NaN", () => {
  for (const provider_cost of [null, undefined, Number.NaN, Infinity, -1, "0.001"]) {
    const value = contentUpdateFeedback(result({ provider_cost }));
    assert.match(value.message, /本次费用未返回/);
    assert.doesNotMatch(value.message, /NaN|Infinity|\$0|未产生付费/);
  }
  assert.match(contentUpdateFeedback(result({ provider_cost: 0.000001 })).message, /\$0\.000001/);
  assert.match(contentUpdateFeedback(result({ provider_cost: 0.001, currency: "CNY" })).message, /费用币种未能确认/);
});

test("optional legacy response fields and unknown field names remain readable", () => {
  assert.match(contentUpdateFeedback({ status: "succeeded", provider_cost: 0 }).message, /^数据已更新/);
  const value = contentUpdateFeedback(result({ metrics: { status: "partial", missing_fields: ["new_internal_counter"] } }));
  assert.match(value.message, /部分指标尚无新数据/);
  assert.doesNotMatch(value.message, /new_internal_counter/);
});
