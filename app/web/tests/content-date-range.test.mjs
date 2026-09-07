import assert from "node:assert/strict";
import test from "node:test";
import { build } from "esbuild";

const bundle = await build({ entryPoints: [new URL("../app/contents/contentDateRange.ts", import.meta.url).pathname], bundle: true, write: false, platform: "node", format: "esm" });
const { todayInShanghai, isCalendarDate, shiftDate, monthOf, shiftMonth, dateInMonth, daysInMonth, moveDateByMonth, mondayOffset, inclusiveDays, dateRangeError, publicationPresets, rangeSummary } = await import(`data:text/javascript;base64,${Buffer.from(bundle.outputFiles[0].text).toString("base64")}`);

test("Shanghai date changes at 16:00 UTC independently of the host timezone", () => {
  assert.equal(todayInShanghai(new Date("2026-09-06T15:59:59Z")), "2026-09-06");
  assert.equal(todayInShanghai(new Date("2026-09-06T16:00:00Z")), "2026-09-07");
  assert.equal(todayInShanghai(new Date("2025-12-31T16:00:00Z")), "2026-01-01");
});

test("manual date validation rejects rollover dates, malformed inputs and non-leap February 29", () => {
  for (const value of ["", "2026-9-07", "2026-02-29", "2026-04-31", "2026-13-01", "2026-00-01", "2026-09-00", "0000-01-01", "2026-09-07x"]) assert.equal(isCalendarDate(value), false, value);
  for (const value of ["2024-02-29", "2000-02-29", "2026-09-07", "0001-01-01"]) assert.equal(isCalendarDate(value), true, value);
});

test("ranges permit the same day but reject missing, reversed and future dates", () => {
  const today = "2026-09-07";
  assert.equal(dateRangeError(today, today, today), "");
  assert.match(dateRangeError("", today, today), /开始日期和结束日期/);
  assert.match(dateRangeError("2026-02-29", today, today), /有效日期/);
  assert.match(dateRangeError(today, "2026-09-06", today), /早于/);
  assert.match(dateRangeError(today, "2026-09-08", today), /未来/);
  assert.match(dateRangeError("2026-09-08", "2026-09-08", today), /未来/);
});

test("inclusive day and month presets include today except yesterday and last month", () => {
  const presets = publicationPresets("2026-09-07");
  assert.deepEqual(presets.map(({ key }) => key), ["today", "yesterday", "last7", "last30", "thisMonth", "lastMonth"]);
  assert.deepEqual(presets.map(({ start, end }) => [start, end]), [
    ["2026-09-07", "2026-09-07"], ["2026-09-06", "2026-09-06"], ["2026-09-01", "2026-09-07"],
    ["2026-08-09", "2026-09-07"], ["2026-09-01", "2026-09-07"], ["2026-08-01", "2026-08-31"],
  ]);
  assert.equal(inclusiveDays(presets[2].start, presets[2].end), 7);
  assert.equal(inclusiveDays(presets[3].start, presets[3].end), 30);
});

test("presets cross years and handle month starts and leap-year February", () => {
  const january = publicationPresets("2026-01-01");
  assert.deepEqual(january.find(({ key }) => key === "thisMonth"), { key: "thisMonth", label: "本月", start: "2026-01-01", end: "2026-01-01" });
  assert.deepEqual(january.find(({ key }) => key === "lastMonth"), { key: "lastMonth", label: "上月", start: "2025-12-01", end: "2025-12-31" });
  assert.equal(january.find(({ key }) => key === "last7").start, "2025-12-26");
  assert.equal(publicationPresets("2024-03-01").find(({ key }) => key === "lastMonth").end, "2024-02-29");
  assert.equal(publicationPresets("2025-03-01").find(({ key }) => key === "lastMonth").end, "2025-02-28");
});

test("calendar keyboard movement clamps short months without rolling into March", () => {
  assert.equal(moveDateByMonth("2026-01-31", 1), "2026-02-28");
  assert.equal(moveDateByMonth("2024-01-31", 1), "2024-02-29");
  assert.equal(moveDateByMonth("2024-02-29", 12), "2025-02-28");
  assert.equal(moveDateByMonth("2026-01-31", -1), "2025-12-31");
  assert.equal(shiftDate("2026-01-01", -1), "2025-12-31");
  assert.equal(shiftDate("0001-01-01", -1), "0001-01-01");
  assert.equal(shiftDate("9999-12-31", 1), "9999-12-31");
  assert.equal(mondayOffset("2026-09-07"), 0);
  assert.equal(mondayOffset("2026-09-06"), 6);
});

test("month iteration and inclusive counts remain stable across timezones and leap days", () => {
  assert.deepEqual(shiftMonth(monthOf("2026-01-01"), -1), { year: 2025, month: 12 });
  assert.equal(dateInMonth({ year: 2024, month: 2 }, daysInMonth({ year: 2024, month: 2 })), "2024-02-29");
  assert.equal(inclusiveDays("2024-02-28", "2024-03-01"), 3);
  assert.equal(inclusiveDays("2026-09-07", "2026-09-07"), 1);
  assert.equal(inclusiveDays("2026-09-08", "2026-09-07"), 0);
  assert.equal(inclusiveDays("", ""), 0);
});

test("selected field stays concise without making cross-year ranges ambiguous", () => {
  assert.equal(rangeSummary("", ""), "");
  assert.equal(rangeSummary("2026-09-07", "2026-09-07"), "2026/09/07");
  assert.equal(rangeSummary("2026-09-01", "2026-09-07"), "2026/09/01 – 09/07");
  assert.equal(rangeSummary("2025-12-30", "2026-01-01"), "2025/12/30 – 2026/01/01");
});
