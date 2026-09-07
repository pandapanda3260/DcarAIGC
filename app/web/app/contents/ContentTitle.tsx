"use client";

import { useId, useLayoutEffect, useRef, useState } from "react";

type Props = { text: string; href: string };
type TitleLayout = { prefix: string; truncated: boolean };
type LayoutCache = { entries: Map<string, TitleLayout>; expiry: ReturnType<typeof setTimeout> | null };
const layoutCaches = new WeakMap<FontFaceSet, LayoutCache>();
const segmenter = new Intl.Segmenter("zh-CN", { granularity: "grapheme" });
const layoutCacheLimit = 256;

function layoutCache(fonts: FontFaceSet) {
  let cache = layoutCaches.get(fonts);
  if (!cache) {
    cache = { entries: new Map(), expiry: null };
    layoutCaches.set(fonts, cache);
    // Remain subscribed while no title is mounted, so navigation never revives old font metrics.
    const current = cache;
    const clear = () => {
      current.entries.clear();
      if (current.expiry !== null) clearTimeout(current.expiry);
      current.expiry = null;
    };
    fonts.addEventListener("loadingdone", clear);
    fonts.addEventListener("loadingerror", clear);
  }
  return cache;
}

function layoutKey(root: HTMLSpanElement, text: string, width: number) {
  const rootStyle = getComputedStyle(root);
  const linkStyle = getComputedStyle(root.querySelector(".content-title-text") ?? root);
  return JSON.stringify([
    text, width, rootStyle.lineHeight, rootStyle.getPropertyValue("--list-action-size"),
    ...["font-family", "font-size", "font-weight", "font-style", "font-stretch", "font-variant", "font-kerning", "font-feature-settings", "font-variation-settings", "font-optical-sizing", "letter-spacing", "word-spacing", "text-transform", "word-break", "overflow-wrap", "white-space", "direction", "writing-mode", "hyphens"]
      .map((property) => linkStyle.getPropertyValue(property)),
  ]);
}

export default function ContentTitle(props: Props) {
  return <MeasuredTitle key={`${props.href}\n${props.text}`} {...props} />;
}

function MeasuredTitle({ text, href }: Props) {
  const titleId = useId();
  const rootRef = useRef<HTMLSpanElement>(null);
  const [expanded, setExpanded] = useState(false);
  const [layout, setLayout] = useState<TitleLayout | null>(null);

  useLayoutEffect(() => {
    const root = rootRef.current;
    if (!root) return;
    const cache = layoutCache(document.fonts);
    let characters: string[] | undefined;
    const prefixAt = (length: number) => characters!.slice(0, length).join("").replace(/[\s.…，,。；;、：:]+$/u, "");
    let lastKey = "";
    let disposed = false;
    let frame = 0;

    const measure = (force = false) => {
      if (disposed) return;
      const width = root.getBoundingClientRect().width;
      if (width <= 0) return;
      const key = layoutKey(root, text, width);
      if (!force && key === lastKey) return;
      lastKey = key;
      const cacheable = document.fonts.status === "loaded" && text.length <= 4096;
      const cached = cacheable ? cache.entries.get(key) : undefined;
      if (cached) {
        cache.entries.delete(key);
        cache.entries.set(key, cached);
        setLayout((previous) => previous?.prefix === cached.prefix && previous.truncated === cached.truncated ? previous : cached);
        return;
      }
      // Keep the probe inside the same table/container-query context as the title.
      const probe = root.cloneNode(false) as HTMLSpanElement;
      probe.removeAttribute("id");
      probe.removeAttribute("data-expanded");
      probe.dataset.measured = "true";
      probe.classList.add("content-title-measure");
      probe.setAttribute("aria-hidden", "true");
      probe.inert = true;
      probe.style.width = `${width}px`;
      const link = document.createElement("a");
      link.className = "content-title-text";
      const tail = document.createElement("span");
      tail.className = "content-title-tail";
      const ellipsis = document.createElement("span");
      ellipsis.className = "content-title-ellipsis";
      ellipsis.textContent = "…";
      const button = document.createElement("button");
      button.className = "content-title-toggle";
      button.textContent = "展开";
      tail.appendChild(ellipsis);
      tail.appendChild(button);
      probe.appendChild(link);
      root.appendChild(probe);
      const limit = parseFloat(getComputedStyle(root).lineHeight) * 2 + 0.5;
      const fits = () => probe.getBoundingClientRect().height <= limit && probe.scrollWidth <= width + 0.5;
      try {
        link.textContent = text;
        let prefix = text;
        const truncated = !fits();
        if (truncated) {
          characters ??= Array.from(segmenter.segment(text), ({ segment }) => segment);
          probe.appendChild(tail);
          let low = 0;
          let high = characters.length;
          while (low < high) {
            const middle = Math.ceil((low + high) / 2);
            link.textContent = prefixAt(middle);
            if (fits()) low = middle;
            else high = middle - 1;
          }
          prefix = prefixAt(low);
        }
        const measured = { prefix, truncated };
        if (cacheable && document.fonts.status === "loaded") {
          cache.entries.delete(key);
          cache.entries.set(key, measured);
          if (cache.entries.size > layoutCacheLimit) cache.entries.delete(cache.entries.keys().next().value!);
          // Keep only a short in-memory window of already rendered title text.
          if (cache.expiry === null) cache.expiry = setTimeout(() => { cache.entries.clear(); cache.expiry = null; }, 60_000);
        }
        setLayout((previous) => previous?.prefix === prefix && previous.truncated === truncated ? previous : measured);
      } finally {
        probe.remove();
      }
    };
    const schedule = () => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(() => measure(true));
    };
    measure(true);
    const observer = new ResizeObserver(() => measure());
    observer.observe(root);
    // An already-resolved ready promise would needlessly force a second full table measurement.
    if (document.fonts.status !== "loaded") document.fonts.ready.then(() => { if (!disposed) schedule(); });
    document.fonts.addEventListener("loadingdone", schedule);
    document.fonts.addEventListener("loadingerror", schedule);
    window.addEventListener("resize", schedule);
    return () => {
      disposed = true;
      cancelAnimationFrame(frame);
      observer.disconnect();
      document.fonts.removeEventListener("loadingdone", schedule);
      document.fonts.removeEventListener("loadingerror", schedule);
      window.removeEventListener("resize", schedule);
    };
  }, [text]);

  return (
    <span className="content-title" ref={rootRef} data-measured={layout ? "true" : undefined} data-expanded={expanded ? "true" : undefined}>
      <a id={titleId} className="content-title-text" href={href} target="_blank" rel="noreferrer" aria-label={text}>{expanded ? text : layout?.prefix ?? text}</a>
      {layout?.truncated && (
        <span className="content-title-tail">
          {!expanded && <span className="content-title-ellipsis" aria-hidden="true">…</span>}
          <button type="button" className="content-title-toggle" aria-expanded={expanded} aria-controls={titleId} onClick={() => setExpanded((value) => !value)}>{expanded ? "收起" : "展开"}</button>
        </span>
      )}
    </span>
  );
}
