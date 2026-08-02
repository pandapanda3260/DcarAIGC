import type { ReactNode } from "react";

export function Loading({ label = "正在读取 v8 运营数据" }: { label?: string }) {
  return <div className="loading-screen"><div className="loading-mark">D</div><p>{label}</p></div>;
}

export function Notice({ tone = "success", children }: { tone?: "success" | "error"; children: ReactNode }) {
  return <div className={`notice ${tone === "error" ? "error-notice" : "success-notice"}`}><span>{tone === "error" ? "!" : "✓"}</span>{children}</div>;
}

export function Feedback({ error, message, onClose }: { error?: string; message?: string; onClose?: () => void }) {
  const text = error || message;
  if (!text) return null;
  return <div className={`notice ${error ? "error-notice" : "success-notice"}`}><span>{error ? "!" : "✓"}</span>{text}{onClose && <button onClick={onClose} aria-label="关闭提示">×</button>}</div>;
}
