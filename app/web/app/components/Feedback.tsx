"use client";

import { useEffect, useId, useRef, useSyncExternalStore } from "react";
import type { ReactNode } from "react";

export function Loading({ label = "正在加载运营数据" }: { label?: string }) {
  // 行内渲染在 .main-area 内：加载指示器居中于"正在加载的内容区"，而不是整个视口
  //（侧边栏仍可用，不属于加载区域；参见 Carbon/Red Hat 设计系统的 loading 规范）。
  // 居中由 globals.css 的 .main-area:has(> .loading-screen) 布局完成，无需 portal/fixed。
  return <div className="loading-screen" role="status" aria-live="polite"><div className="loading-mark">D</div><p>{label}</p></div>;
}

// ---- 全局浮动提示（toast）----
// 提示以浮层形式悬浮在内容区顶部：不占版面（页面内容不会被推下去）、相同文案只出现一条
// （重复触发时重放入场动画代替叠加）、到时自动消失、每条都可手动关闭、悬停时暂停计时。
// Notice / Feedback 保持原签名，内部改为派发 toast，各页面调用处无需改动。

type ToastTone = "success" | "error";
type ToastItem = {
  id: number;
  tone: ToastTone;
  content: ReactNode;
  dedupeKey: string;
  bump: number; // 相同提示再次触发时 +1，key 变化令入场动画重放
  leaving: boolean;
  onClose?: () => void;
};

const TOAST_DURATION: Record<ToastTone, number> = { success: 4000, error: 8000 };
const TOAST_LEAVE_MS = 180;
const TOAST_MAX_VISIBLE = 4;

let toastSeq = 0;
let toastItems: ToastItem[] = [];
let toastsPaused = false;
const toastListeners = new Set<() => void>();
const toastHideTimers = new Map<number, ReturnType<typeof setTimeout>>();
const NO_TOASTS: ToastItem[] = [];

function emitToasts() { for (const listener of toastListeners) listener(); }
function subscribeToasts(listener: () => void) { toastListeners.add(listener); return () => { toastListeners.delete(listener); }; }
function getToasts() { return toastItems; }
function getServerToasts() { return NO_TOASTS; }

function scheduleToastHide(id: number, tone: ToastTone) {
  const pending = toastHideTimers.get(id);
  if (pending) { clearTimeout(pending); toastHideTimers.delete(id); }
  if (toastsPaused) return;
  toastHideTimers.set(id, setTimeout(() => beginToastLeave(id), TOAST_DURATION[tone]));
}

function beginToastLeave(id: number) {
  const pending = toastHideTimers.get(id);
  if (pending) { clearTimeout(pending); toastHideTimers.delete(id); }
  const item = toastItems.find((toast) => toast.id === id);
  if (!item || item.leaving) return;
  toastItems = toastItems.map((toast) => toast.id === id ? { ...toast, leaving: true } : toast);
  emitToasts();
  setTimeout(() => {
    toastItems = toastItems.filter((toast) => toast.id !== id);
    emitToasts();
  }, TOAST_LEAVE_MS);
  item.onClose?.();
}

export function showToast(tone: ToastTone, content: ReactNode, options?: { dedupeKey?: string; onClose?: () => void }): number {
  const fallbackKey = typeof content === "string" || typeof content === "number" ? `${tone}:${content}` : `anonymous:${toastSeq + 1}`;
  const dedupeKey = options?.dedupeKey ?? fallbackKey;
  const existing = toastItems.find((toast) => toast.dedupeKey === dedupeKey && !toast.leaving);
  if (existing) {
    toastItems = toastItems.map((toast) => toast.id === existing.id
      ? { ...toast, content, bump: toast.bump + 1, onClose: options?.onClose ?? toast.onClose }
      : toast);
    scheduleToastHide(existing.id, tone);
    emitToasts();
    return existing.id;
  }
  const id = ++toastSeq;
  toastItems = [...toastItems, { id, tone, content, dedupeKey, bump: 0, leaving: false, onClose: options?.onClose }];
  scheduleToastHide(id, tone);
  const active = toastItems.filter((toast) => !toast.leaving);
  if (active.length > TOAST_MAX_VISIBLE) beginToastLeave(active[0].id);
  emitToasts();
  return id;
}

export function dismissToast(id: number) { beginToastLeave(id); }

function pauseToastTimers() {
  toastsPaused = true;
  for (const timer of toastHideTimers.values()) clearTimeout(timer);
  toastHideTimers.clear();
}

function resumeToastTimers() {
  toastsPaused = false;
  for (const toast of toastItems) { if (!toast.leaving) scheduleToastHide(toast.id, toast.tone); }
}

export function ToastViewport() {
  const items = useSyncExternalStore(subscribeToasts, getToasts, getServerToasts);
  if (!items.length) return null;
  return <div className="toast-viewport" onMouseEnter={pauseToastTimers} onMouseLeave={resumeToastTimers}>
    {items.map((item) => <div
      key={`${item.id}:${item.bump}`}
      className={`toast ${item.tone === "error" ? "toast-error" : "toast-success"}${item.leaving ? " toast-leaving" : ""}`}
      role={item.tone === "error" ? "alert" : "status"}
    >
      <span className="toast-icon" aria-hidden="true">{item.tone === "error" ? "!" : "✓"}</span>
      <div className="toast-text">{item.content}</div>
      <button className="toast-close" type="button" aria-label="关闭提示" onClick={() => dismissToast(item.id)}>×</button>
    </div>)}
  </div>;
}

export function Notice({ tone = "success", children }: { tone?: ToastTone; children: ReactNode }) {
  const anonymousKey = useId();
  const text = typeof children === "string" || typeof children === "number" ? String(children) : null;
  const dedupeKey = text != null ? `${tone}:${text}` : `notice:${anonymousKey}`;
  const contentRef = useRef(children);
  useEffect(() => { contentRef.current = children; });
  useEffect(() => {
    showToast(tone, text ?? contentRef.current, { dedupeKey });
  }, [tone, text, dedupeKey]);
  return null;
}

export function Feedback({ error, message, onClose }: { error?: string; message?: string; onClose?: () => void }) {
  const text = error || message || "";
  const tone: ToastTone = error ? "error" : "success";
  const onCloseRef = useRef(onClose);
  useEffect(() => { onCloseRef.current = onClose; });
  useEffect(() => {
    if (!text) return;
    showToast(tone, text, { onClose: () => onCloseRef.current?.() });
  }, [tone, text]);
  return null;
}
