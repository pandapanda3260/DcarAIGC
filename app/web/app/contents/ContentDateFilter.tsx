"use client";

import { useId, useLayoutEffect, useRef, useState, type KeyboardEvent } from "react";
import { createPortal } from "react-dom";
import {
  dateInMonth, dateRangeError, daysInMonth, inclusiveDays, isCalendarDate,
  mondayOffset, monthKey, monthOf, moveDateByMonth, publicationPresets,
  rangeSummary, shiftDate, shiftMonth, todayInShanghai, type CalendarMonth,
} from "./contentDateRange";
import styles from "./ContentDateFilter.module.css";

const WEEKDAYS = ["一", "二", "三", "四", "五", "六", "日"];
type Props = { start: string; end: string; onChange: (start: string, end: string) => void };

export default function ContentDateFilter({ start, end, onChange }: Props) {
  const id = useId();
  const triggerRef = useRef<HTMLButtonElement>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const startRef = useRef<HTMLInputElement>(null);
  const endRef = useRef<HTMLInputElement>(null);
  const pendingDateFocus = useRef<string | null>(null);
  const [open, setOpen] = useState(false);
  const [today, setToday] = useState("");
  const [draftStart, setDraftStart] = useState(start);
  const [draftEnd, setDraftEnd] = useState(end);
  const [view, setView] = useState<CalendarMonth>({ year: 2000, month: 1 });
  const [singleMonth, setSingleMonth] = useState(false);
  const [position, setPosition] = useState({ top: 0, left: 0 });
  const [ready, setReady] = useState(false);
  const [pickingEnd, setPickingEnd] = useState(false);
  const [hoverDay, setHoverDay] = useState<string | null>(null);
  const [focusedDay, setFocusedDay] = useState("");
  const [showError, setShowError] = useState(false);
  const [presetKey, setPresetKey] = useState("");

  function close(restoreFocus = true) {
    setOpen(false);
    if (restoreFocus) triggerRef.current?.focus();
  }

  function openPicker() {
    if (open) { close(); return; }
    const localToday = todayInShanghai();
    const initial = isCalendarDate(start) && start <= localToday ? start : localToday;
    const narrow = (window.visualViewport?.width ?? window.innerWidth) < 620;
    setToday(localToday);
    const matchingPresets = publicationPresets(localToday).filter((preset) => preset.start === start && preset.end === end);
    setPresetKey(matchingPresets.find((preset) => preset.key === presetKey)?.key ?? matchingPresets[0]?.key ?? "");
    setDraftStart(start);
    setDraftEnd(end);
    setSingleMonth(narrow);
    setView(initialView(initial, localToday, narrow));
    setFocusedDay(initial);
    setPickingEnd(false);
    setHoverDay(null);
    setShowError(false);
    setReady(false);
    setOpen(true);
  }

  function initialView(date: string, currentDay = today, narrow = singleMonth): CalendarMonth {
    const month = monthOf(date);
    return !narrow && monthKey(month) >= monthKey(monthOf(currentDay)) ? shiftMonth(month, -1) : month;
  }

  useLayoutEffect(() => {
    if (!open) return;
    const panel = panelRef.current;
    const trigger = triggerRef.current;
    if (!panel || !trigger) return;
    function place() {
      if (!panel || !trigger) return;
      const viewport = window.visualViewport;
      const viewportWidth = viewport?.width ?? window.innerWidth;
      const viewportHeight = viewport?.height ?? window.innerHeight;
      const viewportLeft = viewport?.offsetLeft ?? 0;
      const viewportTop = viewport?.offsetTop ?? 0;
      panel.style.maxWidth = `${Math.max(0, viewportWidth - 24)}px`;
      panel.style.maxHeight = `${Math.max(0, viewportHeight - 24)}px`;
      const anchor = trigger.getBoundingClientRect();
      const box = panel.getBoundingClientRect();
      setSingleMonth(viewportWidth < 620);
      const left = Math.max(viewportLeft + 12, Math.min(anchor.left, viewportLeft + viewportWidth - box.width - 12));
      const below = anchor.bottom + 8;
      const top = below + box.height <= viewportTop + viewportHeight - 12
        ? below
        : Math.max(viewportTop + 12, anchor.top - box.height - 8);
      setPosition((previous) => previous.top === top && previous.left === left ? previous : { top, left });
      setReady(true);
    }
    place();
    const observer = new ResizeObserver(place);
    observer.observe(panel);
    window.addEventListener("resize", place);
    window.addEventListener("scroll", place, true);
    window.visualViewport?.addEventListener("resize", place);
    window.visualViewport?.addEventListener("scroll", place);
    startRef.current?.focus({ preventScroll: true });
    function onOutside(event: PointerEvent) {
      if (event.target instanceof Node && !panel?.contains(event.target) && !rootRef.current?.contains(event.target)) close(false);
    }
    function onEscape(event: globalThis.KeyboardEvent) {
      if (event.key !== "Escape") return;
      event.preventDefault();
      event.stopPropagation();
      close();
    }
    document.addEventListener("pointerdown", onOutside);
    document.addEventListener("keydown", onEscape, true);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", place);
      window.removeEventListener("scroll", place, true);
      window.visualViewport?.removeEventListener("resize", place);
      window.visualViewport?.removeEventListener("scroll", place);
      document.removeEventListener("pointerdown", onOutside);
      document.removeEventListener("keydown", onEscape, true);
    };
  }, [open]);

  useLayoutEffect(() => {
    if (!open || !pendingDateFocus.current) return;
    const date = pendingDateFocus.current;
    pendingDateFocus.current = null;
    panelRef.current?.querySelector<HTMLButtonElement>(`button[data-date="${date}"]`)?.focus({ preventScroll: true });
  }, [open, focusedDay, view, singleMonth]);

  function focusDate(value: string) {
    const date = value > today ? today : value;
    const month = monthOf(date);
    const last = shiftMonth(view, singleMonth ? 0 : 1);
    if (monthKey(month) < monthKey(view) || monthKey(month) > monthKey(last)) setView(month);
    setFocusedDay(date);
    pendingDateFocus.current = date;
    // If the date is unchanged React need not render (e.g. Right on today).
    panelRef.current?.querySelector<HTMLButtonElement>(`button[data-date="${date}"]`)?.focus({ preventScroll: true });
  }

  function moveDay(event: KeyboardEvent<HTMLButtonElement>, date: string) {
    let next: string | undefined;
    if (event.key === "ArrowLeft") next = shiftDate(date, -1);
    if (event.key === "ArrowRight") next = shiftDate(date, 1);
    if (event.key === "ArrowUp") next = shiftDate(date, -7);
    if (event.key === "ArrowDown") next = shiftDate(date, 7);
    if (event.key === "Home") next = shiftDate(date, -mondayOffset(date));
    if (event.key === "End") next = shiftDate(date, 6 - mondayOffset(date));
    if (event.key === "PageUp") next = moveDateByMonth(date, event.shiftKey ? -12 : -1);
    if (event.key === "PageDown") next = moveDateByMonth(date, event.shiftKey ? 12 : 1);
    if (next) { event.preventDefault(); focusDate(next); }
  }

  function pickDay(date: string) {
    setPresetKey("");
    if (!pickingEnd || !isCalendarDate(draftStart)) {
      setDraftStart(date);
      setDraftEnd("");
      setPickingEnd(true);
    } else {
      setDraftStart(date < draftStart ? date : draftStart);
      setDraftEnd(date < draftStart ? draftStart : date);
      setPickingEnd(false);
    }
    setFocusedDay(date);
    setHoverDay(null);
    setShowError(false);
  }

  function editDate(which: "start" | "end", value: string) {
    setPresetKey("");
    if (which === "start") setDraftStart(value); else setDraftEnd(value);
    setPickingEnd(which === "end");
    setHoverDay(null);
    setShowError(false);
    if (isCalendarDate(value) && value <= today) { setView(initialView(value)); setFocusedDay(value); }
  }

  function clear() {
    onChange("", "");
    close();
  }

  function apply() {
    const error = dateRangeError(draftStart, draftEnd, todayInShanghai());
    if (error) { setShowError(true); return; }
    onChange(draftStart, draftEnd);
    close();
  }

  function keepFocus(event: KeyboardEvent<HTMLDivElement>) {
    if (event.key !== "Tab") return;
    const elements = Array.from(panelRef.current?.querySelectorAll<HTMLElement>('button:not(:disabled):not([tabindex="-1"]), input:not(:disabled), [tabindex="0"]') ?? []);
    const first = elements[0];
    const last = elements[elements.length - 1];
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  }

  const error = dateRangeError(draftStart, draftEnd, today);
  const presets = today ? publicationPresets(today) : [];
  const validDraft = !error;
  const previewEnd = pickingEnd && hoverDay ? hoverDay : draftEnd;
  const displayStart = draftStart && previewEnd && previewEnd < draftStart ? previewEnd : draftStart;
  const displayEnd = draftStart && previewEnd && previewEnd < draftStart ? draftStart : previewEnd;
  const summary = rangeSummary(start, end);
  const lastVisible = shiftMonth(view, singleMonth ? 0 : 1);
  const tabDate = monthKey(monthOf(focusedDay)) >= monthKey(view) && monthKey(monthOf(focusedDay)) <= monthKey(lastVisible)
    ? focusedDay : dateInMonth(view, 1);

  function renderMonth(month: CalendarMonth) {
    const first = dateInMonth(month, 1);
    const cells: Array<string | null> = [
      ...Array.from({ length: mondayOffset(first) }, () => null),
      ...Array.from({ length: daysInMonth(month) }, (_, index) => dateInMonth(month, index + 1)),
    ];
    while (cells.length % 7) cells.push(null);
    const headingId = `${id}-month-${month.year}-${month.month}`;
    return <section className={styles.month} key={headingId} aria-labelledby={headingId}>
      <h3 id={headingId}>{month.year}年{month.month}月</h3>
      <table className={styles.calendar} role="grid" aria-labelledby={headingId}>
        <thead><tr>{WEEKDAYS.map((day) => <th key={day} scope="col" abbr={`星期${day}`}>{day}</th>)}</tr></thead>
        <tbody>{Array.from({ length: cells.length / 7 }, (_, week) => <tr key={week}>
          {cells.slice(week * 7, week * 7 + 7).map((date, offset) => date == null
            ? <td key={`empty-${offset}`} />
            : <td key={date} aria-selected={Boolean(displayStart && displayEnd && date >= displayStart && date <= displayEnd)}>
              <button type="button" className={styles.day} data-date={date}
                data-edge={date === displayStart || date === displayEnd || undefined}
                data-range={Boolean(displayStart && displayEnd && date > displayStart && date < displayEnd) || undefined}
                data-today={date === today || undefined}
                aria-label={`${date}${date === today ? "，今天" : ""}${date === draftStart ? "，开始日期" : ""}${date === draftEnd ? "，结束日期" : ""}`}
                aria-current={date === today ? "date" : undefined}
                disabled={date > today} tabIndex={date === tabDate ? 0 : -1}
                onFocus={() => setFocusedDay(date)} onClick={() => pickDay(date)} onKeyDown={(event) => moveDay(event, date)}
                onMouseEnter={() => { if (pickingEnd && date <= today) setHoverDay(date); }}
              >{Number(date.slice(-2))}</button>
            </td>)}
        </tr>)}</tbody>
      </table>
    </section>;
  }

  return <>
    <div className={styles.control} ref={rootRef} data-active={Boolean(summary) || undefined}>
      <button type="button" ref={triggerRef} className={styles.trigger} onClick={openPicker}
        aria-haspopup="dialog" aria-expanded={open} aria-controls={open ? `${id}-panel` : undefined}
        aria-label={summary ? `发布时间：${summary}，修改日期范围` : "发布时间：不限，选择日期范围"}>
        <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><rect x="3" y="4.5" width="14" height="12.5" rx="2" /><path d="M3 8.5h14M7 2.5v4M13 2.5v4" /></svg>
        <span className={styles.triggerLabel}>发布时间</span>
        {summary && <span className={styles.value}>{summary}</span>}
        <svg className={styles.caret} viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true"><path d="m4 6 4 4 4-4" /></svg>
      </button>
      {(start || end) && <button type="button" className={styles.clear} aria-label="清除发布时间筛选" title="清除发布时间筛选" onClick={clear}><svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true"><path d="m4 4 8 8M12 4l-8 8" /></svg></button>}
    </div>
    {open && createPortal(<div id={`${id}-panel`} ref={panelRef} className={styles.panel} role="dialog" aria-labelledby={`${id}-heading`} aria-describedby={`${id}-help`}
      style={{ top: position.top, left: position.left, visibility: ready ? "visible" : "hidden" }} onKeyDown={keepFocus}>
      <header className={styles.header}><h2 id={`${id}-heading`}>发布时间</h2><button type="button" className={styles.close} aria-label="取消日期选择" onClick={() => close()}>×</button></header>
      <div className={styles.body}>
        <div className={styles.presets} aria-label="快捷日期范围">{presets.map((preset) => <button type="button" key={preset.key}
          aria-pressed={presetKey === preset.key && draftStart === preset.start && draftEnd === preset.end} onClick={() => {
            setPresetKey(preset.key);
            setDraftStart(preset.start); setDraftEnd(preset.end); setView(initialView(preset.start));
            setFocusedDay(preset.start); setPickingEnd(false); setHoverDay(null); setShowError(false);
          }}>{preset.label}</button>)}</div>
        <div className={styles.inputs}>
          <label htmlFor={`${id}-start`}>开始日期<input ref={startRef} id={`${id}-start`} type="text" inputMode="numeric" placeholder="YYYY-MM-DD" autoComplete="off" maxLength={10}
            value={draftStart} onChange={(event) => editDate("start", event.target.value)} onFocus={() => setPickingEnd(false)} onBlur={() => setShowError(true)}
            aria-invalid={showError && Boolean(draftStart) && (!isCalendarDate(draftStart) || draftStart > today)} aria-describedby={`${id}-help ${id}-status`}
            onKeyDown={(event) => { if (event.key === "ArrowDown") { event.preventDefault(); focusDate(isCalendarDate(draftStart) && draftStart <= today ? draftStart : today); } }} /></label>
          <span className={styles.separator} aria-hidden="true">—</span>
          <label htmlFor={`${id}-end`}>结束日期<input ref={endRef} id={`${id}-end`} type="text" inputMode="numeric" placeholder="YYYY-MM-DD" autoComplete="off" maxLength={10}
            value={draftEnd} onChange={(event) => editDate("end", event.target.value)} onFocus={() => setPickingEnd(true)} onBlur={() => setShowError(true)}
            aria-invalid={showError && Boolean(draftEnd) && (!isCalendarDate(draftEnd) || draftEnd > today || Boolean(draftStart && draftEnd < draftStart))} aria-describedby={`${id}-help ${id}-status`}
            onKeyDown={(event) => { if (event.key === "ArrowDown") { event.preventDefault(); focusDate(isCalendarDate(draftEnd) && draftEnd <= today ? draftEnd : today); } }} /></label>
        </div>
        <div className={styles.calendarArea} onMouseLeave={() => setHoverDay(null)}>
          <nav className={styles.navigation} aria-label="切换日历月份"><button type="button" aria-label="上一个月" disabled={monthKey(view) <= 12} onClick={() => setView(shiftMonth(view, -1))}>‹</button><button type="button" aria-label="下一个月" disabled={monthKey(lastVisible) >= monthKey(monthOf(today))} onClick={() => setView(shiftMonth(view, 1))}>›</button></nav>
          <div className={styles.months} data-single={singleMonth || undefined}>{renderMonth(view)}{!singleMonth && renderMonth(shiftMonth(view, 1))}</div>
        </div>
        <p id={`${id}-help`} className={styles.help}>按发布时间筛选，包含起止当天（上海时间）。</p>
        <p id={`${id}-status`} className={styles.status} data-error={showError && Boolean(error) || undefined} aria-live="polite">{showError && error ? error : validDraft ? `已选 ${inclusiveDays(draftStart, draftEnd)} 天，点击应用生效` : pickingEnd ? "请选择结束日期，可与开始日期相同" : "输入日期或点击日历选择范围"}</p>
      </div>
      <footer className={styles.footer}><button type="button" className={styles.reset} onClick={clear}>清除</button><div><button type="button" className={styles.cancel} onClick={() => close()}>取消</button><button type="button" className={styles.apply} disabled={!validDraft} onClick={apply}>应用</button></div></footer>
    </div>, document.body)}
  </>;
}
