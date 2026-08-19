"use client";

import type { ReactNode } from "react";

export function Loading({ label = "正在加载运营数据" }: { label?: string }) {
  // 行内渲染在 .main-area 内：加载指示器居中于"正在加载的内容区"，而不是整个视口
  //（侧边栏仍可用，不属于加载区域；参见 Carbon/Red Hat 设计系统的 loading 规范）。
  // 居中由 globals.css 的 .main-area:has(> .loading-screen) 布局完成，无需 portal/fixed。
  return <div className="loading-screen" role="status" aria-live="polite"><div className="loading-mark">D</div><p>{label}</p></div>;
}

export function Notice({ tone = "success", children }: { tone?: "success" | "error"; children: ReactNode }) {
  return <div className={`notice ${tone === "error" ? "error-notice" : "success-notice"}`} role={tone === "error" ? "alert" : "status"}><span>{tone === "error" ? "!" : "✓"}</span>{children}</div>;
}

export function Feedback({ error, message, onClose }: { error?: string; message?: string; onClose?: () => void }) {
  const text = error || message;
  if (!text) return null;
  return <div className={`notice feedback-notice ${error ? "error-notice" : "success-notice"}`} role={error ? "alert" : "status"}><span>{error ? "!" : "✓"}</span>{text}{onClose && <button onClick={onClose} aria-label="关闭提示">×</button>}</div>;
}
