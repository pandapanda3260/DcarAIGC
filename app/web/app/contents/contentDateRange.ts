export type DateRange = { start: string; end: string };
export type DatePreset = DateRange & { key: string; label: string };
export type CalendarMonth = { year: number; month: number };

const DAY_MS = 86_400_000;
const shanghaiDate = new Intl.DateTimeFormat("en-CA", {
  timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
});

/** Use calendar dates throughout; the reader applies Shanghai day boundaries. */
export function todayInShanghai(now = new Date()): string {
  const parts = shanghaiDate.formatToParts(now);
  const part = (type: string) => parts.find((item) => item.type === type)?.value ?? "";
  return `${part("year")}-${part("month")}-${part("day")}`;
}

function utcDate(year: number, month: number, day: number): Date {
  const date = new Date(0);
  date.setUTCFullYear(year, month - 1, day);
  date.setUTCHours(0, 0, 0, 0);
  return date;
}

export function isCalendarDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const [year, month, day] = value.split("-").map(Number);
  if (year < 1 || month < 1 || month > 12 || day < 1 || day > 31) return false;
  const date = utcDate(year, month, day);
  return date.getUTCFullYear() === year && date.getUTCMonth() + 1 === month && date.getUTCDate() === day;
}

export function shiftDate(value: string, days: number): string {
  if (!isCalendarDate(value)) throw new RangeError("Invalid calendar date");
  const [year, month, day] = value.split("-").map(Number);
  const date = utcDate(year, month, day + days);
  // The input and calendar use four-digit ISO years.
  if (date.getUTCFullYear() < 1) return "0001-01-01";
  if (date.getUTCFullYear() > 9999) return "9999-12-31";
  return date.toISOString().slice(0, 10);
}

export function monthOf(value: string): CalendarMonth {
  const [year, month] = value.split("-").map(Number);
  return { year, month };
}

export function monthKey(view: CalendarMonth): number {
  return view.year * 12 + view.month - 1;
}

export function shiftMonth(view: CalendarMonth, delta: number): CalendarMonth {
  const index = Math.max(12, Math.min(9999 * 12 + 11, monthKey(view) + delta));
  return { year: Math.floor(index / 12), month: index % 12 + 1 };
}

export function dateInMonth(view: CalendarMonth, day: number): string {
  return `${String(view.year).padStart(4, "0")}-${String(view.month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

export function daysInMonth(view: CalendarMonth): number {
  return utcDate(view.year, view.month + 1, 0).getUTCDate();
}

/** Keep the day where possible, clamping Jan 31 -> Feb 28/29. */
export function moveDateByMonth(value: string, delta: number): string {
  const target = shiftMonth(monthOf(value), delta);
  return dateInMonth(target, Math.min(Number(value.slice(-2)), daysInMonth(target)));
}

export function mondayOffset(value: string): number {
  const [year, month, day] = value.split("-").map(Number);
  return (utcDate(year, month, day).getUTCDay() + 6) % 7;
}

export function inclusiveDays(start: string, end: string): number {
  if (!isCalendarDate(start) || !isCalendarDate(end) || end < start) return 0;
  return Math.round((Date.parse(`${end}T00:00:00Z`) - Date.parse(`${start}T00:00:00Z`)) / DAY_MS) + 1;
}

export function dateRangeError(start: string, end: string, today: string): string {
  if (!start || !end) return "请选择开始日期和结束日期";
  if (!isCalendarDate(start) || !isCalendarDate(end)) return "请输入有效日期，格式为 YYYY-MM-DD";
  if (start > today || end > today) return "不能选择未来日期";
  if (end < start) return "结束日期不能早于开始日期";
  return "";
}

export function publicationPresets(today: string): DatePreset[] {
  const current = monthOf(today);
  const previous = shiftMonth(current, -1);
  const yesterday = shiftDate(today, -1);
  return [
    { key: "today", label: "今天", start: today, end: today },
    { key: "yesterday", label: "昨天", start: yesterday, end: yesterday },
    { key: "last7", label: "近7天", start: shiftDate(today, -6), end: today },
    { key: "last30", label: "近30天", start: shiftDate(today, -29), end: today },
    { key: "thisMonth", label: "本月", start: dateInMonth(current, 1), end: today },
    { key: "lastMonth", label: "上月", start: dateInMonth(previous, 1), end: dateInMonth(previous, daysInMonth(previous)) },
  ];
}

export function rangeSummary(start: string, end: string): string {
  if (!isCalendarDate(start) || !isCalendarDate(end)) return "";
  if (start === end) return start.replaceAll("-", "/");
  const ending = start.slice(0, 4) === end.slice(0, 4) ? end.slice(5) : end;
  return `${start.replaceAll("-", "/")} – ${ending.replaceAll("-", "/")}`;
}
