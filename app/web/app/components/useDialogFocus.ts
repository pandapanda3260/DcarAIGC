"use client";

import { useEffect, useEffectEvent, type RefObject } from "react";

// 弹窗焦点管理（逻辑与 SpuAudiencePage 的刷新弹窗一致）：打开时记住触发元素、锁住 body 滚动、
// 把焦点放到弹窗内第一个可聚焦元素；Escape 关闭（提交中不响应）；Tab 在弹窗内循环；关闭后恢复滚动与焦点。
// Effect Event 读取最新的 onClose / busy / initialFocus：effect 只依赖 open，
// 不会因为每次输入重新绑定并把焦点拉回第一个控件。
const FOCUSABLE = 'button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [href], [tabindex]:not([tabindex="-1"])';

export function useDialogFocus(
  open: boolean,
  dialogRef: RefObject<HTMLElement | null>,
  options: { onClose: () => void; busy: boolean; initialFocus?: string },
) {
  const closeIfIdle = useEffectEvent(() => {
    if (!options.busy) options.onClose();
  });
  const initialFocusSelector = useEffectEvent(() => options.initialFocus);

  useEffect(() => {
    if (!open) return;
    const previouslyFocused = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const previousBodyOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const focusFrame = window.requestAnimationFrame(() => {
      const dialog = dialogRef.current;
      if (!dialog) return;
      const selector = initialFocusSelector();
      const target = (selector ? dialog.querySelector<HTMLElement>(selector) : null) ?? dialog.querySelector<HTMLElement>(FOCUSABLE) ?? dialog;
      target.focus();
    });
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        closeIfIdle();
        return;
      }
      if (event.key !== "Tab") return;
      const dialog = dialogRef.current;
      if (!dialog) return;
      const focusable = Array.from(dialog.querySelectorAll<HTMLElement>(FOCUSABLE));
      if (focusable.length === 0) {
        event.preventDefault();
        dialog.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault(); last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault(); first.focus();
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = previousBodyOverflow;
      window.requestAnimationFrame(() => previouslyFocused?.focus());
    };
  }, [open, dialogRef]);
}
