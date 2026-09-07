import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";
import {
  mediaAge, mediaBlockerLabel, mediaBytes, mediaDeadlineText, mediaManualPage,
  mediaMemberKey, mediaMemberLabel, mediaPresentation, mediaPlaybackPresentation,
} from "../app/lib/mediaLifecycle.ts";
import { ApiRequestError, requestMediaRestore } from "../app/lib/api.ts";

const bundleId = "a".repeat(32);
const before = Date.parse("2026-08-29T03:59:59Z");
const due = "2026-08-29T04:00:00Z";

function original(index, overrides = {}) {
  return { artifact_id: 41, bundle_id: bundleId, member_id: "member-" + index,
    index, kind: "image", name: "image-" + index + ".jpg",
    url: "/api/v8/contents/17/evidence/files/41/" + index, available: true, ...overrides };
}
function bundle(overrides = {}) {
  return {
    content: { id: 17 }, read_only: false,
    media: [original(0), original(2, { available: false }), original(5)],
    previews: [],
    media_availability: { status: "available", reason: "原件可用。" },
    media_lifecycle: {
      bundle_id: bundleId, state: "hot", operation_state: "idle",
      reason: "original_available", http_status: 200, read_only: false,
      can_restore: false, can_reprocess: true,
      archive_verified_at: "2026-08-26T04:00:00Z", delete_due_at: due,
      deleted_at: null, last_error: null,
    }, ...overrides,
  };
}
function archived(overrides = {}) {
  const value = bundle();
  return bundle({ media_lifecycle: { ...value.media_lifecycle, state: "archived",
    reason: "original_archived", http_status: 409, can_restore: true, can_reprocess: false, ...overrides } });
}

beforeEach((t) => {
  t.mock.method(globalThis, "fetch", () => { throw new Error("Real network forbidden in media tests"); });
});

test("member identities and missing original indices never compact or borrow another image", () => {
  const value = bundle();
  const view = mediaPresentation(value, before);
  assert.deepEqual(view.originalMembers.map((item) => item.index), [0, 2, 5]);
  assert.deepEqual(view.gallery.map((item) => item.index), [0, 5]);
  assert.equal(mediaMemberLabel(view.originalMembers[1]), "原件 3");
  assert.equal(mediaMemberKey(view.originalMembers[2]), bundleId + ":41:5");
  assert.ok(view.originalMembers[2].url.endsWith("/5"));
  assert.equal(view.originalMembers[1].available, false);
  assert.deepEqual(value.media.map((item) => item.index), [0, 2, 5]);
});

test("preview identity and URL remain separate from their original member", () => {
  const value = archived();
  value.previews = [{ ...original(3), artifact_id: 91, original_index: 5,
    url: "/api/v8/contents/17/evidence/previews/91/3" }];
  const view = mediaPresentation(value, before);
  assert.equal(view.isPreview, true);
  assert.equal(view.originalsAvailable, false);
  assert.equal(view.gallery[0].url, "/api/v8/contents/17/evidence/previews/91/3");
  assert.equal(mediaMemberLabel(view.gallery[0], true), "预览 4 · 对应原件 6");
  assert.equal(view.originalMembers[2].url, "/api/v8/contents/17/evidence/files/41/5");
});

test("playback chooses readable video originals over retained still previews", () => {
  const video = original(0, { kind: "video", name: "source.mp4" });
  const preview = original(0, { artifact_id: 91, original_index: 0,
    url: "/api/v8/contents/17/evidence/previews/91/0" });
  const value = bundle({ media: [video], previews: [preview] });
  const view = mediaPlaybackPresentation(value, before);
  assert.deepEqual(view.gallery, [video]);
  assert.equal(view.isPreview, false);
  // The evidence workbench keeps its existing preview-first presentation.
  assert.deepEqual(mediaPresentation(value, before).gallery, [preview]);
});

test("playback falls back to bound previews without reopening restricted originals", () => {
  const preview = original(0, { artifact_id: 91, original_index: 0 });
  const value = archived();
  value.previews = [preview, original(1, { bundle_id: "old-source" })];
  for (const restricted of [value, { ...value, read_only: true },
    bundle({ previews: [preview], read_only: true }),
    bundle({ previews: [preview], media_lifecycle: { ...value.media_lifecycle,
      state: "hot", reason: "original_available", http_status: 200, delete_due_at: due } })]) {
    const at = restricted === value ? before : Date.parse(due) + 1;
    const view = mediaPlaybackPresentation(restricted, at);
    assert.deepEqual(view.gallery, [preview]);
    assert.equal(view.isPreview, true);
  }
});

test("playback preserves member indices and excludes missing or foreign originals", () => {
  const value = bundle();
  value.media.push(original(7, { bundle_id: "old-source" }));
  const view = mediaPlaybackPresentation(value, before);
  assert.deepEqual(view.gallery.map((item) => item.index), [0, 5]);
  assert.equal(view.isPreview, false);
});

test("legacy readable replica media stays readable without enabling writes", () => {
  const value = bundle({ media_lifecycle: null, read_only: true });
  const view = mediaPresentation(value, before);
  assert.equal(view.originalsAvailable, true);
  assert.deepEqual(view.gallery.map((item) => item.index), [0, 5]);
  assert.equal(view.canRestore, false);
  assert.equal(view.canReprocess, false);
});

test("old source bundle previews are never used for a newly selected bundle", () => {
  const value = archived();
  value.previews = [original(0, { bundle_id: "b".repeat(32) })];
  value.media.push(original(7, { bundle_id: "b".repeat(32) }));
  const view = mediaPresentation(value, before);
  assert.deepEqual(view.gallery, []);
  assert.deepEqual(view.originalMembers.map((item) => item.index), [0, 2, 5]);
});

test("local restoration is allowed only before the exact deadline", () => {
  const value = archived();
  assert.equal(mediaPresentation(value, before).canRestore, true);
  for (const at of [Date.parse(due), Date.parse(due) + 1000]) {
    const view = mediaPresentation(value, at);
    assert.equal(view.canRestore, false);
    assert.equal(view.canReprocess, false);
    assert.equal(view.reason, "original_expiry_pending");
    assert.match(view.title, /等待安全删除/);
    assert.equal(value.media_lifecycle.delete_due_at, due);
  }
  assert.equal(mediaPresentation(archived({ delete_due_at: null }), before).canRestore, false);
  assert.equal(mediaPresentation(archived({ delete_due_at: "invalid" }), before).canRestore, false);
});

test("purging and partial deletion failure never re-enable restoration", () => {
  const value = archived({ operation_state: "purging", last_error: "unlink_failed", can_restore: true });
  const view = mediaPresentation(value, before);
  assert.equal(view.canRestore, false);
  assert.equal(view.canReprocess, false);
  assert.equal(view.reason, "original_purge_in_progress");
  assert.match(view.title, /删除失败.*结算/);
  assert.doesNotMatch(view.title, /已到期删除/);
});

test("expired originals remain non-replayable while retained previews stay visible", () => {
  const value = archived({ state: "expired", deleted_at: due, reason: "original_expired", http_status: 410 });
  value.previews = [original(0, { artifact_id: 91, url: "/api/v8/contents/17/evidence/previews/91/0" })];
  const view = mediaPresentation(value, Date.parse(due) + 1000);
  assert.equal(view.canRestore, false);
  assert.equal(view.canReprocess, false);
  assert.equal(view.canReacquire, false);
  assert.equal(view.gallery.length, 1);
  assert.equal(view.reason, "original_expired");
});

test("read-only snapshots cannot promise local original availability or restoration", () => {
  const value = archived({ read_only: true, can_restore: true });
  const view = mediaPresentation(value, before);
  assert.equal(view.originalsAvailable, false);
  assert.equal(view.canRestore, false);
  assert.equal(view.canReprocess, false);
  assert.equal(view.canReacquire, false);
  assert.equal(view.restoring, false);
});

test("restoring state is distinct from archived and does not submit another restore", () => {
  const value = archived({ operation_state: "restoring", reason: "original_restoring", http_status: 202 });
  const view = mediaPresentation(value, before);
  assert.equal(view.restoring, true);
  assert.equal(view.canRestore, false);
  assert.equal(view.canReprocess, false);
});

test("unknown sizes are never rendered as zero and deadline text never introduces another retention phase", () => {
  for (const value of [null, undefined, NaN, -1, Infinity]) assert.equal(mediaBytes(value), "未知");
  assert.equal(mediaBytes(0), "0 B");
  assert.equal(mediaBytes(1024), "1.00 KiB");
  assert.equal(mediaBytes(1048576), "1.00 MiB");
  assert.equal(mediaAge(null), "未知");
  assert.equal(mediaAge(14 * 86400), "14 天 0 小时");
  assert.equal(mediaDeadlineText(due, Date.parse(due)), "已到原定删除时间；延迟不延长期限");
  assert.doesNotMatch(mediaDeadlineText(due, before), /回收区|宽限|延期/);
});

test("manual todo paging is oldest-first and never clears protected or evidence-ready records", () => {
  const items = Array.from({ length: 23 }, (_, index) => ({
    bundle_id: String(index).padStart(32, "0"), registered_at: "2026-08-" + String(1 + index).padStart(2, "0") + "T00:00:00Z",
    protected: true, evidence_ready: index % 2 === 0, registered_bytes: index === 0 ? null : 1024,
    blockers: ["formal_V2_V3_evaluation_missing"], resolution: index === 0 ? "人工已阅，仍保留保护" : null,
  })).reverse();
  const first = mediaManualPage(items, 1, 20);
  const second = mediaManualPage(items, 2, 20);
  assert.equal(first.total, 23);
  assert.equal(first.pages, 2);
  assert.equal(first.items[0].bundle_id, "0".repeat(32));
  assert.equal(first.items[0].registered_bytes, null);
  assert.equal(first.items[0].protected, true);
  assert.equal(first.items[0].evidence_ready, true);
  assert.equal(first.items[0].resolution, "人工已阅，仍保留保护");
  assert.equal(second.items.length, 3);
  assert.equal(mediaManualPage(items, 999, 20).page, 2);
  assert.ok(items[0].registered_at.startsWith("2026-08-23"));
});

test("known blockers explain the missing evidence without exposing internal file paths", () => {
  assert.equal(mediaBlockerLabel("formal_V2_V3_evaluation_missing"), "正式媒体评估尚未完成");
  assert.equal(mediaBlockerLabel("purge_failed"), "删除失败，等待安全结算");
  assert.equal(mediaBlockerLabel("awaiting_retention_worker"), "等待下一轮删除作业");
  assert.equal(mediaBlockerLabel("/private/secret/runtime.sqlite3"), "本地处理异常，待管理员核查");
  assert.equal(mediaBlockerLabel(null), "原因尚未核实");
});

test("explicit evidence and reprocess restoration each send one bound POST with no paid-refresh option", async (t) => {
  const calls = [];
  t.mock.method(globalThis, "fetch", async (path, init) => {
    calls.push({ path, init });
    const body = JSON.parse(init.body);
    return Response.json({ run_id: 72, bundle_id: body.bundle_id, purpose: body.purpose,
      status: "pending", http_status: 202, provider_cost: 0 }, { status: 202 });
  });
  for (const purpose of ["evidence", "reprocess"]) {
    const receipt = await requestMediaRestore(17, bundleId, purpose);
    assert.equal(receipt.run_id, 72);
    assert.equal(receipt.provider_cost, 0);
    assert.equal(calls.at(-1).path, "/api/v8/contents/17/media/restore");
    assert.equal(calls.at(-1).init.method, "POST");
    assert.deepEqual(JSON.parse(calls.at(-1).init.body), { bundle_id: bundleId, purpose });
  }
  assert.equal(calls.length, 2);
});

test("409 and 410 restoration refusals preserve machine reasons and never automatically retry", async (t) => {
  for (const [status, code] of [[409, "original_expiry_pending"], [409, "original_purge_in_progress"],
    [410, "original_expired"], [409, "replica_original_omitted"]]) {
    let count = 0;
    t.mock.method(globalThis, "fetch", async () => {
      count += 1;
      return Response.json({ detail: "internal detail", code }, { status });
    });
    await assert.rejects(requestMediaRestore(17, bundleId, "evidence"), (error) =>
      error instanceof ApiRequestError && error.status === status && error.code === code && !error.retryable);
    assert.equal(count, 1);
  }
});

test("a 202 without a durable identity and purpose-bound run does not claim successful restoration", async (t) => {
  let count = 0;
  for (const body of [{ bundle_id: bundleId, http_status: 202 },
    { bundle_id: bundleId, http_status: 202, run_id: 72, purpose: "reprocess" }]) {
    t.mock.method(globalThis, "fetch", async () => {
      count += 1;
      return Response.json(body, { status: 202 });
    });
    await assert.rejects(requestMediaRestore(17, bundleId, "evidence"),
      (error) => error instanceof ApiRequestError && error.code === "restore_receipt_invalid" && !error.retryable);
  }
  assert.equal(count, 2);
});

test("invalid restore identity or purpose is rejected before any network request", async (t) => {
  const network = t.mock.method(globalThis, "fetch", () => { throw new Error("must not dispatch"); });
  for (const args of [[0, bundleId, "evidence"], [17, "../wrong", "evidence"], [17, bundleId, "paid_refresh"]]) {
    await assert.rejects(requestMediaRestore(...args), ApiRequestError);
  }
  assert.equal(network.mock.calls.length, 0);
});
