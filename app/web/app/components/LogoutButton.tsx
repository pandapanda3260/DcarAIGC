"use client";

import { useState } from "react";
import { publicAssetPath } from "../lib/paths";

export default function LogoutButton() {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const logout = async () => {
    if (busy) return;
    setBusy(true);
    setError("");

    try {
      const headers = { "X-Dcar-Request": "logout" };
      const response = await fetch(publicAssetPath("/auth/logout"), {
        method: "POST",
        headers,
        credentials: "same-origin",
        cache: "no-store",
      });
      if (!response.ok) throw new Error(`logout failed: ${response.status}`);
      const payload = await response.json() as { redirect_to?: string };
      window.location.replace(payload.redirect_to ?? publicAssetPath("/login"));
    } catch {
      setBusy(false);
      setError("退出失败，请稍后重试");
    }
  };

  return (
    <>
      {error && <span id="logout-error" className="sidebar-logout-error" role="alert">{error}</span>}
      <button
        type="button"
        className="sidebar-logout"
        title={busy ? "正在退出" : "退出登录"}
        aria-label={busy ? "正在退出登录" : "退出登录"}
        aria-describedby={error ? "logout-error" : undefined}
        onClick={() => void logout()}
        disabled={busy}
      >
        <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false">
          <path d="M12.5 6.3V5.1a1.8 1.8 0 0 0-1.8-1.8H5.4a1.8 1.8 0 0 0-1.8 1.8v9.8a1.8 1.8 0 0 0 1.8 1.8h5.3a1.8 1.8 0 0 0 1.8-1.8v-1.2" />
          <path d="M8.2 10h8.3M13.9 7.4 16.5 10l-2.6 2.6" />
        </svg>
      </button>
    </>
  );
}
