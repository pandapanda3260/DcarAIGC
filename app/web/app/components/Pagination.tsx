"use client";

import { useState } from "react";

export const PAGE_SIZE_OPTIONS = [20, 50, 100];

export function pageWindow(current: number, totalPages: number): Array<number | "left-gap" | "right-gap"> {
  if (totalPages <= 7) return Array.from({ length: totalPages }, (_, index) => index + 1);
  const middle = [current - 1, current, current + 1].filter((value, index, list) => list.indexOf(value) === index && value > 1 && value < totalPages);
  const slots: Array<number | "left-gap" | "right-gap"> = [1];
  if (middle.length && middle[0] > 2) slots.push("left-gap");
  slots.push(...middle);
  if (middle.length && middle[middle.length - 1] < totalPages - 1) slots.push("right-gap");
  slots.push(totalPages);
  return slots;
}

type PaginationProps = {
  page: number;
  pageSize: number;
  total: number;
  busy?: boolean;
  ariaLabel: string;
  unitLabel?: string;
  placement?: "top" | "bottom";
  onChange: (next: { page: number; pageSize?: number }) => void;
};

export function Pagination({ page, pageSize, total, busy = false, ariaLabel, unitLabel = "条", placement = "bottom", onChange }: PaginationProps) {
  const [jumpValue, setJumpValue] = useState("");
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  function goToPage(next: number) {
    const target = Math.min(Math.max(1, next), totalPages);
    if (target === page || busy) return;
    onChange({ page: target });
  }
  function jumpToPage() {
    const parsed = Number.parseInt(jumpValue, 10);
    setJumpValue("");
    if (Number.isNaN(parsed)) return;
    goToPage(parsed);
  }
  return <div className={`pagination-bar pagination-${placement}`}>
    <span className="pagination-info">共 {total} {unitLabel} · 第 {page} / {totalPages} 页</span>
    <nav className="pagination-controls" aria-label={ariaLabel}>
      <button className="page-button" disabled={busy || page <= 1} onClick={() => goToPage(1)}>首页</button>
      <button className="page-button" disabled={busy || page <= 1} onClick={() => goToPage(page - 1)}>上一页</button>
      {pageWindow(page, totalPages).map((slot) => typeof slot === "number"
        ? <button key={slot} className={`page-button${slot === page ? " active" : ""}`} disabled={busy} aria-current={slot === page ? "page" : undefined} onClick={() => goToPage(slot)}>{slot}</button>
        : <span key={slot} className="page-ellipsis">…</span>)}
      <button className="page-button" disabled={busy || page >= totalPages} onClick={() => goToPage(page + 1)}>下一页</button>
      <button className="page-button" disabled={busy || page >= totalPages} onClick={() => goToPage(totalPages)}>末页</button>
    </nav>
    <div className="pagination-jump">
      <label>每页<select value={pageSize} disabled={busy} onChange={(event) => onChange({ page: 1, pageSize: Number(event.target.value) })}>{PAGE_SIZE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}</select>条</label>
      <label>跳至<input value={jumpValue} disabled={busy} inputMode="numeric" placeholder="页码" onChange={(event) => setJumpValue(event.target.value.replace(/[^0-9]/g, ""))} onKeyDown={(event) => { if (event.key === "Enter") jumpToPage(); }} />页</label>
      <button className="page-button" disabled={busy || !jumpValue} onClick={jumpToPage}>跳转</button>
    </div>
  </div>;
}
