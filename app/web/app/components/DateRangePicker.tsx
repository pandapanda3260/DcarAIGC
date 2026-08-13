"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { formatDate } from "../lib/format";

const SHANGHAI_TZ = "Asia/Shanghai";
const WEEKDAYS = ["一", "二", "三", "四", "五", "六", "日"];

type MonthView = { year: number; month: number };

export function todayInShanghai(): string {
  return new Date().toLocaleDateString("en-CA", { timeZone: SHANGHAI_TZ });
}

export function shiftDays(iso: string, days: number): string {
  const [year, month, day] = iso.split("-").map(Number);
  return new Date(Date.UTC(year, month - 1, day + days)).toISOString().slice(0, 10);
}

export function dayCount(start: string, end: string): number {
  const span = Date.parse(`${end}T00:00:00Z`) - Date.parse(`${start}T00:00:00Z`);
  return Math.round(span / 86_400_000) + 1;
}

function isoOf(year: number, month: number, day: number): string {
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function monthViewOf(iso: string): MonthView {
  const [year, month] = iso.split("-").map(Number);
  return { year, month };
}

function addMonths(view: MonthView, delta: number): MonthView {
  const index = view.year * 12 + (view.month - 1) + delta;
  return { year: Math.floor(index / 12), month: (index % 12) + 1 };
}

function monthIndex(view: MonthView): number {
  return view.year * 12 + (view.month - 1);
}

function daysInMonth(view: MonthView): number {
  return new Date(Date.UTC(view.year, view.month, 0)).getUTCDate();
}

function mondayFirstOffset(view: MonthView): number {
  return (new Date(Date.UTC(view.year, view.month - 1, 1)).getUTCDay() + 6) % 7;
}

export type PresetRange = { key: string; label: string; start: string; end: string };

export function buildPresets(today: string): PresetRange[] {
  const yesterday = shiftDays(today, -1);
  const { year, month } = monthViewOf(today);
  const monthStart = isoOf(year, month, 1);
  const previous = addMonths({ year, month }, -1);
  const presets: PresetRange[] = [
    { key: "today", label: "今天", start: today, end: today },
    { key: "yesterday", label: "昨天", start: yesterday, end: yesterday },
    { key: "last7", label: "最近7天", start: shiftDays(today, -7), end: yesterday },
    { key: "last14", label: "最近14天", start: shiftDays(today, -14), end: yesterday },
    { key: "last30", label: "最近30天", start: shiftDays(today, -30), end: yesterday },
    {
      key: "thisMonth",
      label: "本月",
      start: monthStart,
      end: yesterday >= monthStart ? yesterday : today,
    },
    {
      key: "lastMonth",
      label: "上月",
      start: isoOf(previous.year, previous.month, 1),
      end: isoOf(previous.year, previous.month, daysInMonth(previous)),
    },
  ];
  return presets.filter((preset) => preset.start <= preset.end);
}

export default function DateRangePicker({ start, end, onChange, disabled }: {
  start: string;
  end: string;
  onChange: (start: string, end: string) => void;
  disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [view, setView] = useState<MonthView>(() => monthViewOf(start));
  const [anchor, setAnchor] = useState<string | null>(null);
  const [hover, setHover] = useState<string | null>(null);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const today = todayInShanghai();
  const presets = useMemo(() => buildPresets(today), [today]);
  const visibleOpen = open && !disabled;

  useEffect(() => () => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
  }, []);

  useEffect(() => {
    if (!visibleOpen) return;
    function onPointerDown(event: MouseEvent) {
      if (rootRef.current && event.target instanceof Node && !rootRef.current.contains(event.target)) {
        setAnchor(null);
        setHover(null);
        setOpen(false);
      }
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key !== "Escape") return;
      event.stopPropagation();
      if (anchor != null) {
        setAnchor(null);
        setHover(null);
      } else {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKeyDown, true);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown, true);
    };
  }, [visibleOpen, anchor]);

  function toggle() {
    if (disabled) return;
    if (open) {
      setAnchor(null);
      setHover(null);
      setOpen(false);
      return;
    }
    setView(monthViewOf(start));
    setAnchor(null);
    setHover(null);
    setOpen(true);
  }

  function commit(rangeStart: string, rangeEnd: string, delayClose: boolean) {
    onChange(rangeStart, rangeEnd);
    setAnchor(null);
    setHover(null);
    if (delayClose) {
      if (closeTimer.current) clearTimeout(closeTimer.current);
      closeTimer.current = setTimeout(() => setOpen(false), 180);
    } else {
      setOpen(false);
    }
  }

  function pickDay(iso: string) {
    if (anchor == null) {
      setAnchor(iso);
      setHover(iso);
      return;
    }
    commit(anchor <= iso ? anchor : iso, anchor <= iso ? iso : anchor, true);
  }

  const displayStart = anchor != null ? (hover != null && hover < anchor ? hover : anchor) : start;
  const displayEnd = anchor != null ? (hover != null && hover > anchor ? hover : anchor) : end;
  const activePreset = anchor == null
    ? presets.find((preset) => preset.start === start && preset.end === end)?.key
    : undefined;
  const canGoNext = monthIndex(addMonths(view, 1)) <= monthIndex(monthViewOf(today));

  function renderMonth(month: MonthView, side: "left" | "right") {
    const total = daysInMonth(month);
    const offset = mondayFirstOffset(month);
    const cells: Array<string | null> = [
      ...Array.from({ length: offset }, () => null),
      ...Array.from({ length: total }, (_, index) => isoOf(month.year, month.month, index + 1)),
    ];
    return <div className="drp-month">
      <div className="drp-month-head">
        {side === "left"
          ? <button type="button" className="drp-nav" onClick={() => setView(addMonths(view, -1))} aria-label="上一个月">‹</button>
          : <span className="drp-nav-spacer" aria-hidden="true" />}
        <span className="drp-month-label">{month.year}年{month.month}月</span>
        {side === "right"
          ? <button type="button" className="drp-nav" onClick={() => setView(addMonths(view, 1))} disabled={!canGoNext} aria-label="下一个月">›</button>
          : <span className="drp-nav-spacer" aria-hidden="true" />}
      </div>
      <div className="drp-grid" role="group" aria-label={`${month.year}年${month.month}月`} onMouseLeave={() => { if (anchor != null) setHover(anchor); }}>
        {WEEKDAYS.map((weekday) => <span key={weekday} className="drp-weekday">{weekday}</span>)}
        {cells.map((iso, index) => iso == null
          ? <span key={`blank-${index}`} className="drp-cell" aria-hidden="true" />
          : <button
              key={iso}
              type="button"
              className={[
                "drp-day",
                iso === displayStart ? "is-start" : "",
                iso === displayEnd ? "is-end" : "",
                iso > displayStart && iso < displayEnd ? "in-range" : "",
                iso === today ? "is-today" : "",
              ].filter(Boolean).join(" ")}
              disabled={iso > today}
              onClick={() => pickDay(iso)}
              onMouseEnter={anchor != null ? () => setHover(iso) : undefined}
              aria-label={formatDate(iso)}
              aria-pressed={iso === displayStart || iso === displayEnd}
            >{Number(iso.slice(-2))}</button>)}
      </div>
    </div>;
  }

  return <div className={`drp${visibleOpen ? " open" : ""}`} ref={rootRef}>
    <button type="button" className="drp-field" onClick={toggle} disabled={disabled} aria-expanded={visibleOpen} aria-haspopup="dialog">
      <span className="drp-field-value">
        <svg className="drp-field-icon" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth={1.6} strokeLinecap="round" aria-hidden="true"><rect x="3" y="4.5" width="14" height="12.5" rx="2" /><path d="M3 8.5h14M7 2.5v3.5M13 2.5v3.5" /></svg>
        {formatDate(start)}<span className="drp-field-sep">—</span>{formatDate(end)}
        <span className="drp-field-days">共 {dayCount(start, end)} 天</span>
      </span>
      <span className="drp-field-caret" aria-hidden="true">{visibleOpen ? "▴" : "▾"}</span>
    </button>
    {visibleOpen && <div className="drp-panel" role="dialog" aria-label="选择报告日期区间">
      <div className="drp-body">
        <div className="drp-presets" role="listbox" aria-label="快捷区间">
          {presets.map((preset) => <button
            key={preset.key}
            type="button"
            className={`drp-preset${activePreset === preset.key ? " active" : ""}`}
            onClick={() => commit(preset.start, preset.end, false)}
            role="option"
            aria-selected={activePreset === preset.key}
          >{preset.label}</button>)}
        </div>
        <div className="drp-calendars">
          {renderMonth(view, "left")}
          {renderMonth(addMonths(view, 1), "right")}
        </div>
      </div>
      <div className="drp-foot">
        <span>{anchor == null
          ? "点击日历选择开始日期，或直接使用左侧快捷区间"
          : "再点击一个日期作为结束日期，可跨月翻页"}</span>
        <span>已选 <strong>{dayCount(displayStart, displayEnd)}</strong> 天</span>
      </div>
    </div>}
  </div>;
}
