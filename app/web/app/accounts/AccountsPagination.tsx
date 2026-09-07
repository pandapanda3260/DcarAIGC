"use client";

import { useRef, useState } from "react";
import { CaretLeftIcon, CaretRightIcon } from "@phosphor-icons/react";
import { PAGE_SIZE_OPTIONS, pageWindow } from "../components/Pagination";
import styles from "./AccountsPagination.module.css";

type AccountsPaginationProps = {
  page: number;
  pageSize: number;
  total: number;
  busy?: boolean;
  onChange: (next: { page: number; pageSize?: number }) => void;
};

export function AccountsPagination({ page, pageSize, total, busy = false, onChange }: AccountsPaginationProps) {
  const [jumpValue, setJumpValue] = useState<string | null>(null);
  const jumpDraft = useRef<string | null>(null);
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  function clearJump() {
    jumpDraft.current = null;
    setJumpValue(null);
  }

  function goToPage(next: number) {
    const target = Math.min(Math.max(1, next), totalPages);
    if (busy || target === page) return;
    onChange({ page: target });
  }

  function commitJump() {
    const draft = jumpDraft.current;
    if (draft === null) return;
    // Clear synchronously so Enter followed by blur submits only once.
    clearJump();
    if (!draft) return;
    goToPage(Number.parseInt(draft, 10));
  }

  return <footer className={styles.pagination}>
    <div className={styles.summary}>
      <span>共 {total} 个账号</span>
      <span className={styles.legend}>— 表示尚未采集</span>
    </div>
    <div className={styles.controls}>
      <select
        className={styles.pageSize}
        aria-label="每页账号数"
        data-pagination-action="true"
        value={pageSize}
        disabled={busy}
        onChange={(event) => {
          const nextPageSize = Number(event.target.value);
          if (!busy && nextPageSize !== pageSize) onChange({ page: 1, pageSize: nextPageSize });
        }}
      >
        {PAGE_SIZE_OPTIONS.map((option) => <option key={option} value={option}>{option} 条/页</option>)}
      </select>
      <nav className={styles.pages} aria-label="账号列表分页">
        <button type="button" className={styles.pageButton} data-pagination-action="true" aria-label="上一页" disabled={busy || page <= 1} onClick={() => goToPage(page - 1)}><CaretLeftIcon size={15} aria-hidden="true" /></button>
        {pageWindow(page, totalPages).map((slot) => typeof slot === "number"
          ? <button type="button" key={slot} className={`${styles.pageButton}${slot === page ? ` ${styles.current}` : ""}`} data-pagination-action="true" aria-label={`第 ${slot} 页`} aria-current={slot === page ? "page" : undefined} disabled={busy} onClick={() => goToPage(slot)}>{slot}</button>
          : <span key={slot} className={styles.ellipsis} aria-hidden="true">…</span>)}
        <button type="button" className={styles.pageButton} data-pagination-action="true" aria-label="下一页" disabled={busy || page >= totalPages} onClick={() => goToPage(page + 1)}><CaretRightIcon size={15} aria-hidden="true" /></button>
      </nav>
      <label className={styles.jump}>跳至
        <input
          aria-label="跳转页码"
          inputMode="numeric"
          pattern="[0-9]*"
          value={jumpValue ?? String(page)}
          disabled={busy}
          onChange={(event) => {
            const value = event.target.value.replace(/[^0-9]/g, "");
            jumpDraft.current = value;
            setJumpValue(value);
          }}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              commitJump();
            }
          }}
          onBlur={(event) => {
            if (event.relatedTarget instanceof HTMLElement && event.relatedTarget.dataset.paginationAction) clearJump();
            else commitJump();
          }}
        />页
      </label>
    </div>
  </footer>;
}
