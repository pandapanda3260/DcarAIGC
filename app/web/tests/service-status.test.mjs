import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { dailyReportStatus, dataServiceStatus } from "../app/lib/serviceStatus.ts";

const healthy = {
  status: "ok", read_only: false,
  automation: { scheduler_state: "running", paid_dispatch_state: "open", report_from_date: "2026-09-06" },
  data_freshness: { status: "current", last_successful_capture_at: "2026-09-06T00:00:00Z" },
};
const snapshot = {
  status: "current", last_verified_install_at: "2026-09-07T03:15:00Z",
  window_state: "active", next_scheduled_at: null,
};

test("service status does not claim healthy before the first response", () => {
  assert.equal(dataServiceStatus(undefined, false).kind, "checking");
});

test("verified current replica sync is healthy and shows install time, independent of writer evidence", () => {
  for (const scheduler_state of ["running", "paused", "stopped", "read_only"]) {
    const state = dataServiceStatus({ ...healthy, read_only: true, snapshot_sync: snapshot,
      automation: { scheduler_state, paid_dispatch_state: "invalid" }, data_freshness: { status: "stale" } }, false);
    assert.equal(state.kind, "online");
    assert.equal(state.label, "快照同步正常");
    assert.match(state.description, /最近同步：2026\/09\/07 11:15（北京时间）/);
    assert.match(state.description, /同步成功不代表采集完整/);
    assert.doesNotMatch(state.description, /最近成功采集|2026\/09\/06/);
  }
});

test("replica sync delay persists outside the publishing window without implying a local capture failure", () => {
  const state = dataServiceStatus({ ...healthy, read_only: true, snapshot_sync: {
    ...snapshot, status: "delayed", window_state: "inactive", next_scheduled_at: "2026-09-08T09:00:00+08:00",
  } }, false);
  assert.equal(state.kind, "stale");
  assert.equal(state.label, "快照同步延迟");
  assert.match(state.description, /00:00–09:00（北京时间）不累计同步延迟/);
  assert.match(state.description, /下次计划发布：2026\/09\/08 09:00（北京时间）/);
  assert.doesNotMatch(state.description, /自动抓取异常|最近成功采集/);
});

test("night window does not downgrade an on-time replica and absent or malformed evidence stays unknown", () => {
  assert.equal(dataServiceStatus({ ...healthy, read_only: true, snapshot_sync: { ...snapshot, window_state: "inactive" } }, false).kind, "online");
  for (const snapshot_sync of [undefined, null, { ...snapshot, status: "unknown" }, { ...snapshot, status: "unexpected" }]) {
    const state = dataServiceStatus({ ...healthy, read_only: true, snapshot_sync }, false);
    assert.equal(state.kind, "unknown");
    assert.equal(state.label, "同步状态待确认");
  }
  for (const status of ["current", "delayed"]) {
    for (const last_verified_install_at of [null, "", "0", "2026-09-07", "2026-09-07T03:15:00", "2026-02-30T03:15:00Z", "invalid"]) {
      const state = dataServiceStatus({ ...healthy, read_only: true, snapshot_sync: { ...snapshot, status, last_verified_install_at } }, false);
      assert.equal(state.kind, "unknown");
      assert.doesNotMatch(state.description, /最近同步：/);
    }
  }
});

test("green status claims a running scheduler with an open gate, nothing more", () => {
  assert.equal(dataServiceStatus(healthy, false).kind, "online");
  assert.equal(dataServiceStatus({ status: "ok", read_only: false }, false).kind, "unknown");
  assert.equal(dataServiceStatus({ status: "unavailable", read_only: false }, false).kind, "offline");
  assert.equal(dataServiceStatus({ status: "ok" }, false).kind, "offline");
  for (const scheduler_state of ["paused", "stopped"]) {
    const state = dataServiceStatus({ ...healthy, automation: { ...healthy.automation, scheduler_state } }, false);
    assert.equal(state.kind, scheduler_state);
    assert.match(state.description, /自动抓取和自动报告/);
  }
  assert.equal(dataServiceStatus({ ...healthy, automation: { scheduler_state: "read_only" } }, false).kind, "unknown");
  assert.equal(dataServiceStatus({ ...healthy, snapshot_sync: snapshot, automation: { scheduler_state: "read_only" } }, false).kind, "online");
  assert.equal(dataServiceStatus({ ...healthy, automation: { ...healthy.automation, paid_dispatch_state: "draining" } }, false).label, "自动抓取已暂停");
  // A sealed coverage receipt is no longer a precondition for green: an unsealed profile day is a
  // normal daily window, and a genuinely outdated day is reported as "stale" instead.
  assert.equal(dataServiceStatus({ ...healthy, automation: { scheduler_state: "running" } }, false).kind, "unknown");
  assert.equal(dataServiceStatus({ ...healthy, data_freshness: { status: "stale" } }, false).kind, "stale");
  for (const data_freshness of [undefined, { status: "unknown" }, { status: "current" }, { status: "current", last_successful_capture_at: "bad date" }]) {
    assert.equal(dataServiceStatus({ ...healthy, data_freshness }, false).kind, "online");
  }
});

test("no state states an unknown freshness or an unknown capture time", () => {
  const states = [
    dataServiceStatus({ ...healthy, data_freshness: { status: "stale" } }, false),
    dataServiceStatus({ status: "ok", read_only: false }, false),
    dataServiceStatus({ ...healthy, read_only: true, data_freshness: { status: "unknown" } }, false),
    dataServiceStatus({ ...healthy, automation: { ...healthy.automation, scheduler_state: "paused" }, data_freshness: undefined }, false),
    dataServiceStatus({ ...healthy, automation: { ...healthy.automation, paid_dispatch_state: "sealed" }, data_freshness: { status: "unknown" } }, false),
  ];
  for (const state of states) {
    assert.doesNotMatch(state.label, /新鲜度/);
    assert.doesNotMatch(state.description, /未知/);
    assert.doesNotMatch(state.description, /最近成功采集/);
  }
  // A writer capture receipt must never be presented as the replica's sync time.
  assert.doesNotMatch(dataServiceStatus({ ...healthy, read_only: true }, false).description, /最近成功采集：|最近同步：/);
});

test("a corrupt paid-dispatch chain is a fault, not a planned pause", () => {
  const invalid = dataServiceStatus({ ...healthy, automation: { ...healthy.automation, paid_dispatch_state: "invalid" } }, false);
  // Only the fault family is painted red; calm "paused" copy would bury a chain that never self-heals.
  assert.equal(invalid.kind, "offline");
  assert.equal(invalid.label, "自动抓取异常");
  assert.match(invalid.description, /联系技术/);
  assert.doesNotMatch(invalid.description, /暂停/);
  for (const paid_dispatch_state of ["draining", "sealed"]) {
    const planned = dataServiceStatus({ ...healthy, automation: { ...healthy.automation, paid_dispatch_state } }, false);
    assert.equal(planned.kind, "paused");
    assert.equal(planned.label, "自动抓取已暂停");
  }
});

test("failed refresh overrides cached healthy or read-only status", () => {
  for (const cached of [undefined, healthy, { ...healthy, read_only: true, snapshot_sync: snapshot }]) {
    assert.equal(dataServiceStatus(cached, true).kind, "offline");
  }
});

test("persistent chrome reads health and shares the derived status with page shells", async () => {
  const shell = (await Promise.all(["AppShell", "WorkbenchChrome"].map((name) =>
    readFile(new URL(`../app/components/${name}.tsx`, import.meta.url), "utf8"),
  ))).join("\n");
  assert.match(shell, /readQueryJson<ServiceHealth>\("\/api\/v8\/health", undefined, 5_000\)/);
  assert.match(shell, /refetchInterval: 30_000/);
  assert.match(shell, /refetchOnWindowFocus: "always"/);
  assert.match(shell, /dataServiceStatus\(serviceHealth.data, serviceHealth.isError\)/);
  // User-selected display policy: failures in the header; all other states stay in the sidebar.
  assert.match(shell, /\{serviceState\.kind === "offline" && <div/);
  assert.doesNotMatch(shell, /serviceState\.kind !== "online"/);
  assert.match(shell, /serviceStyles\.offline/);
  assert.doesNotMatch(shell, /<strong>数据服务正常<\/strong>/);
  assert.match(shell, /title=\{serviceState\.description \|\| undefined\}/);
  assert.match(shell, /aria-label=\{\[serviceState\.label, serviceState\.description\]\.filter\(Boolean\)\.join\("。"\)\}/);
});

test("report gap starts with the first enabled business day and only after 08:00 Shanghai next day", () => {
  for (const at of ["2026-09-06T12:00:00Z", "2026-09-06T23:59:59Z"]) {
    assert.equal(dailyReportStatus([], "2026-09-06", new Date(at)).missingDate, null);
  }
  const due = dailyReportStatus([], "2026-09-06", new Date("2026-09-07T00:00:00Z"));
  assert.equal(due.missingDate, "2026-09-06");
  assert.match(due.message, /当前列表尚无 2026-09-06 日报。/);
  assert.doesNotMatch(due.message, /日报尚未生成/);
  for (const floor of [undefined, null, "", "2026-02-30", "invalid"]) {
    assert.equal(dailyReportStatus([], floor, new Date("2026-09-07T00:00:00Z")).missingDate, null);
  }
});

test("report status distinguishes absent, generating, failed and completed daily reports", () => {
  const task = { task_type: "daily", period_start: "2026-09-06", period_end: "2026-09-06", task_status: "succeeded" };
  const at = new Date("2026-09-07T00:15:00Z");
  for (const task_status of ["queued", "running", "partial", "succeeded"]) {
    const result = dailyReportStatus([{ ...task, task_status }], "2026-09-06", at);
    assert.equal(result.missingDate, null);
    assert.equal(result.failedDate, null);
    assert.equal(result.latestDate, ["partial", "succeeded"].includes(task_status) ? "2026-09-06" : null);
  }
  assert.equal(dailyReportStatus([{ ...task, task_status: "failed" }], "2026-09-06", at).failedDate, "2026-09-06");
  assert.equal(dailyReportStatus([{ ...task, task_type: "custom" }], "2026-09-06", at).missingDate, "2026-09-06");
  assert.equal(dailyReportStatus([{ ...task, period_start: "2026-09-01" }], "2026-09-06", at).latestDate, null);
});

test("idle task lists continue refreshing and refetch on focus while active reports poll quickly", async () => {
  const tasks = await readFile(new URL("../app/tasks/TasksPage.tsx", import.meta.url), "utf8");
  assert.match(tasks, /\.\.\.tasksListQueryOptions\(\),/);
  assert.match(tasks, /refetchInterval: \(query\) => !query.state.error && query.state.data\?\.items.some\(\(task\) => isGeneratingTaskStatus\(task.task_status\)\) \? 1_500 : 30_000/);
  assert.match(tasks, /refetchOnWindowFocus: "always"/);
  assert.match(tasks, /refetchIntervalInBackground: false/);
  assert.match(tasks, /dailyReportStatus\(tasks, serviceHealth.isError \? undefined : serviceHealth.data\?\.automation\?\.report_from_date\)/);
});


test("invalid capture gate wins over writer scheduler states but never over a read-only replica", () => {
  for (const scheduler_state of ["running", "paused", "stopped", undefined]) {
    const health = { ...healthy, automation: { scheduler_state, paid_dispatch_state: "invalid" } };
    assert.equal(dataServiceStatus(health, false).label, "自动抓取异常");
    assert.equal(dataServiceStatus({ ...health, read_only: true }, false).kind, "unknown");
    assert.equal(dataServiceStatus({ ...health, read_only: true, snapshot_sync: snapshot }, false).kind, "online");
  }
});

test("an absent or unrecognized gate never becomes a green light or a planned pause", () => {
  for (const paid_dispatch_state of [undefined, null, "", "unexpected"]) {
    const state = dataServiceStatus({ ...healthy, automation: { ...healthy.automation, paid_dispatch_state } }, false);
    assert.equal(state.kind, "unknown");
    assert.equal(state.label, "自动抓取状态待确认");
    assert.doesNotMatch(state.description, /新鲜度|已暂停/);
  }
});

test("capture timestamps must be real timezone-qualified timestamps, never numeric placeholders", () => {
  for (const last_successful_capture_at of [null, "", "0", "1", "2026-09-06", "bad date", "2026-09-06T00:00:00"]) {
    const health = { ...healthy, data_freshness: { status: "unknown", last_successful_capture_at } };
    assert.equal(dataServiceStatus(health, false).description, "");
    assert.doesNotMatch(dataServiceStatus({ ...health, read_only: true }, false).description, /最近成功采集/);
  }
  assert.match(dataServiceStatus({ ...healthy, data_freshness: { status: "current", last_successful_capture_at: "2026-09-06T00:00:00+00:00" } }, false).description, /2026\/09\/06 08:00/);
});
