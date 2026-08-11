"use client";

import { useEffect, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";

export function Loading({ label = "正在读取 v8 运营数据" }: { label?: string }) {
  // Portal to <body>: .main-area 的 container-type 会产生 layout containment，
  // 使内部 position:fixed 相对容器而非视口定位；挂到 body 才能真正全屏居中。
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  if (!mounted) return null;
  return createPortal(
    <div className="loading-screen" role="status" aria-live="polite"><div className="loading-mark">D</div><p>{label}</p></div>,
    document.body,
  );
}

export function Notice({ tone = "success", children }: { tone?: "success" | "error"; children: ReactNode }) {
  return <div className={`notice ${tone === "error" ? "error-notice" : "success-notice"}`} role={tone === "error" ? "alert" : "status"}><span>{tone === "error" ? "!" : "✓"}</span>{children}</div>;
}

export function Feedback({ error, message, onClose }: { error?: string; message?: string; onClose?: () => void }) {
  const text = error || message;
  if (!text) return null;
  return <div className={`notice ${error ? "error-notice" : "success-notice"}`} role={error ? "alert" : "status"}><span>{error ? "!" : "✓"}</span>{text}{onClose && <button onClick={onClose} aria-label="关闭提示">×</button>}</div>;
}
