"use client";

import { useId, useLayoutEffect, useRef, useState } from "react";

type Props = { text: string; href: string };

export default function ContentTitle(props: Props) {
  return <MeasuredTitle key={`${props.href}\n${props.text}`} {...props} />;
}

function MeasuredTitle({ text, href }: Props) {
  const titleId = useId();
  const rootRef = useRef<HTMLSpanElement>(null);
  const [expanded, setExpanded] = useState(false);
  const [layout, setLayout] = useState<{ prefix: string; truncated: boolean } | null>(null);

  useLayoutEffect(() => {
    const root = rootRef.current;
    if (!root) return;
    const characters = Array.from(new Intl.Segmenter("zh-CN", { granularity: "grapheme" }).segment(text), ({ segment }) => segment);
    const prefixAt = (length: number) => characters.slice(0, length).join("").replace(/[\s.…，,。；;、：:]+$/u, "");
    let lastWidth = 0;
    let disposed = false;
    let frame = 0;

    const measure = (force = false) => {
      const width = root.getBoundingClientRect().width;
      if (disposed || width <= 0 || (!force && Math.abs(width - lastWidth) < 0.25)) return;
      lastWidth = width;
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
        setLayout((previous) => previous?.prefix === prefix && previous.truncated === truncated ? previous : { prefix, truncated });
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
    document.fonts.ready.then(() => { if (!disposed) schedule(); });
    document.fonts.addEventListener("loadingdone", schedule);
    window.addEventListener("resize", schedule);
    return () => {
      disposed = true;
      cancelAnimationFrame(frame);
      observer.disconnect();
      document.fonts.removeEventListener("loadingdone", schedule);
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
