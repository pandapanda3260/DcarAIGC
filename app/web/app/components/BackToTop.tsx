"use client";

import { useEffect, useState, type MouseEvent } from "react";
import { ArrowUpIcon } from "@phosphor-icons/react";
import styles from "./BackToTop.module.css";

function hasVisibleModal() {
  return Array.from(document.querySelectorAll<HTMLElement>('[aria-modal="true"], dialog[open]')).some((dialog) => {
    if (!dialog.getClientRects().length) return false;
    if (typeof dialog.checkVisibility === "function") {
      return dialog.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true });
    }
    for (let element: HTMLElement | null = dialog; element; element = element.parentElement) {
      const style = window.getComputedStyle(element);
      if (element.hidden || style.display === "none" || style.visibility === "hidden" || style.visibility === "collapse" || style.opacity === "0") return false;
    }
    return true;
  });
}

function viewportHeight() {
  return window.innerHeight || document.documentElement.clientHeight;
}

export default function BackToTop({ pageKey, targetId = "main-content" }: { pageKey: string; targetId?: string }) {
  const [visiblePage, setVisiblePage] = useState<string | null>(null);

  useEffect(() => {
    let frame = 0;
    let needsModalCheck = true;
    let modalOpen = false;
    let lastVisible: boolean | undefined;

    const check = () => {
      frame = 0;
      if (needsModalCheck) {
        modalOpen = hasVisibleModal();
        needsModalCheck = false;
      }
      const height = viewportHeight();
      const documentHeight = document.scrollingElement?.scrollHeight ?? document.documentElement.scrollHeight;
      const visible = height > 0 && documentHeight > height && window.scrollY > height && !modalOpen;
      if (visible !== lastVisible) {
        lastVisible = visible;
        setVisiblePage(visible ? pageKey : null);
      }
    };
    const schedule = (checkModals = false) => {
      needsModalCheck ||= checkModals;
      if (!frame) frame = window.requestAnimationFrame(check);
    };
    const onScroll = () => schedule();
    const onLayoutChange = () => schedule(true);

    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", onLayoutChange);
    window.addEventListener("pageshow", onLayoutChange);
    document.addEventListener("transitionend", onLayoutChange, true);
    document.addEventListener("animationend", onLayoutChange, true);

    const mutations = new MutationObserver(onLayoutChange);
    mutations.observe(document.body, {
      subtree: true,
      childList: true,
      attributes: true,
      attributeFilter: ["aria-modal", "aria-hidden", "open", "hidden", "class", "style"],
    });
    const sizes = new ResizeObserver(onLayoutChange);
    sizes.observe(document.documentElement);
    sizes.observe(document.body);
    schedule(true);

    return () => {
      window.cancelAnimationFrame(frame);
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onLayoutChange);
      window.removeEventListener("pageshow", onLayoutChange);
      document.removeEventListener("transitionend", onLayoutChange, true);
      document.removeEventListener("animationend", onLayoutChange, true);
      mutations.disconnect();
      sizes.disconnect();
    };
  }, [pageKey]);

  function backToTop(event: MouseEvent<HTMLButtonElement>) {
    // Recheck at activation so a newly opened modal cannot expose a stale action.
    if (hasVisibleModal() || window.scrollY <= viewportHeight()) return;
    if (event.detail === 0) document.getElementById(targetId)?.focus({ preventScroll: true });
    const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    window.scrollTo({ top: 0, behavior: reduceMotion ? "instant" : "smooth" });
  }

  if (visiblePage !== pageKey) return null;

  return (
    <button className={styles.button} type="button" aria-label="回到顶部" onClick={backToTop}>
      <ArrowUpIcon size={20} weight="bold" aria-hidden="true" />
      <span className={styles.tooltip} aria-hidden="true">回到顶部</span>
    </button>
  );
}
