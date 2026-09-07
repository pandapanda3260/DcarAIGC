import { useEffect, useRef, type RefObject } from "react";

// 模态对话框的键盘与焦点约定（W3C APG dialog 模式），与 SpuAudiencePage 的刷新弹窗同一套行为：
// 打开时记住触发元素并锁定页面滚动、把焦点交给关闭按钮；父文档内 Esc 关闭；Tab 在弹窗内循环；
// 关闭后把焦点还给触发元素。SpuAudiencePage 仍保留其内联实现（其测试断言源码），新弹窗统一走这里。
// 跨源 iframe 的键盘事件不会冒泡到父文档；调用方应在 iframe 后提供可聚焦的关闭出口。

const FOCUSABLE_SELECTOR = [
  "button:not([disabled])", "[href]", "input:not([disabled])", "select:not([disabled])",
  "textarea:not([disabled])", "video[controls]", "iframe", "[tabindex]:not([tabindex=\"-1\"])",
].join(", ");

export function useDialogFocus(
  active: boolean,
  dialogRef: RefObject<HTMLElement | null>,
  onClose: () => void,
  initialSelector = ".modal-close",
) {
  // 关闭回调通常是调用方内联的箭头函数，每次渲染都变；放进 ref 以免主效果反复重建。
  const onCloseRef = useRef(onClose);
  useEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  useEffect(() => {
    if (!active) return;
    const previouslyFocused = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const previousBodyOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    // Keep the dialog's ancestor chain active, but make all background branches
    // inert so focus leaving a child browsing context cannot reach the page.
    const background: Array<{ element: HTMLElement; inert: boolean }> = [];
    let branch: HTMLElement | null = dialogRef.current;
    while (branch && branch !== document.body) {
      const parent: HTMLElement | null = branch.parentElement;
      if (!parent) break;
      for (const sibling of parent.children) {
        if (sibling !== branch && sibling instanceof HTMLElement) {
          background.push({ element: sibling, inert: sibling.inert });
          sibling.setAttribute("inert", "");
        }
      }
      branch = parent;
    }
    const focusFrame = window.requestAnimationFrame(() => {
      const dialog = dialogRef.current;
      if (!dialog) return;
      (dialog.querySelector<HTMLElement>(initialSelector) ?? dialog).focus();
    });
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onCloseRef.current();
        return;
      }
      if (event.key !== "Tab") return;
      const dialog = dialogRef.current;
      if (!dialog) return;
      const focusable = Array.from(dialog.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR))
        .filter((element) => element.tabIndex >= 0 && element.getClientRects().length > 0
          && !element.closest("[inert], [hidden]"));
      if (focusable.length === 0) {
        event.preventDefault();
        dialog.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      const current = document.activeElement;
      const outside = !(current instanceof Node) || !dialog.contains(current);
      if (event.shiftKey && (current === first || outside)) {
        event.preventDefault(); last.focus();
      } else if (!event.shiftKey && (current === last || outside)) {
        event.preventDefault(); first.focus();
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = previousBodyOverflow;
      for (const { element, inert } of background) element.toggleAttribute("inert", inert);
      window.requestAnimationFrame(() => previouslyFocused?.focus());
    };
  }, [active, dialogRef, initialSelector]);
}
