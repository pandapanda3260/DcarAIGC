"use client";

import { useEffect, useRef, useState, type KeyboardEvent } from "react";
import { isAbortError, markedJsonRequest, readQueryJson, requireApprovedSession } from "../lib/api";
import type { AuthSession } from "../lib/types";
import styles from "./PersonalProfile.module.css";
import tooltipStyles from "./QuickActionTooltip.module.css";

export default function InlineNicknameEditor({ session, onCancel, onSaved }: {
  session: AuthSession;
  onCancel: () => void;
  onSaved: (session: AuthSession) => void;
}) {
  // Initialize only on entering edit mode. Session polling must not replace a draft.
  const [initialName] = useState(session.display_name || session.username || "");
  const [name, setName] = useState(initialName);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const inputRef = useRef<HTMLTextAreaElement | null>(null);
  const submitting = useRef(false);
  const composing = useRef(false);
  const changed = name !== initialName;

  useEffect(() => {
    const input = inputRef.current;
    if (!input) return;
    input.focus();
    input.setSelectionRange(input.value.length, input.value.length);
  }, []);

  async function save() {
    if (submitting.current || composing.current || !changed) return;
    submitting.current = true;
    setSaving(true);
    setError("");
    try {
      const updated = await readQueryJson<AuthSession>("/auth/profile", markedJsonRequest({ display_name: name }, "profile-update"), 15_000);
      if (!updated.authenticated || updated.username !== session.username || typeof updated.display_name !== "string") {
        throw new Error("昵称保存结果异常，请重试。");
      }
      await requireApprovedSession(updated);
      onSaved(updated);
    } catch (reason) {
      setError(isAbortError(reason) ? "保存超时，请重试。" : reason instanceof Error ? reason.message : "昵称保存失败，请重试。");
    } finally {
      submitting.current = false;
      setSaving(false);
    }
  }

  function keyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    // Safari may end composition before dispatching its final Enter keydown.
    if (composing.current || event.nativeEvent.isComposing || event.nativeEvent.keyCode === 229) return;
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      void save();
    } else if (event.key === "Escape") {
      event.preventDefault();
      if (!submitting.current) onCancel();
    }
  }

  return <form className={styles.editor} aria-label="编辑昵称" onSubmit={(event) => { event.preventDefault(); void save(); }}>
    <textarea ref={inputRef} className={styles.nicknameInput} aria-label="昵称" aria-describedby={`nickname-editor-help${name === "" ? " nickname-fallback-help" : ""}`} rows={1} wrap="off" style={{ height: Math.min(80, 30 + 20 * (name.split("\n").length - 1)) }} value={name} autoComplete="nickname" placeholder="输入昵称" disabled={saving}
      onChange={(event) => setName(event.target.value)} onKeyDown={keyDown}
      onCompositionStart={() => { composing.current = true; }} onCompositionEnd={() => { composing.current = false; }} />
    <div className={styles.editActions}>
      <button type="submit" className={`${styles.iconButton} ${tooltipStyles.trigger}`} disabled={saving || !changed} aria-label={saving ? "正在保存昵称" : "保存昵称"}>
        {saving ? <span className={styles.saving} aria-hidden="true" /> : <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth={1.7} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="m4 10 4 4 8-8" /></svg>}
        <span className={tooltipStyles.tip} aria-hidden="true">{saving ? "正在保存昵称" : "保存昵称"}</span>
      </button>
      <button type="button" className={`${styles.iconButton} ${tooltipStyles.trigger}`} disabled={saving} onClick={onCancel} aria-label="取消编辑昵称"><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth={1.7} strokeLinecap="round" aria-hidden="true"><path d="m5 5 10 10M15 5 5 15" /></svg><span className={tooltipStyles.tip} aria-hidden="true">取消编辑</span></button>
      <span id="nickname-editor-help" className="visually-hidden">Enter 保存，Esc 取消，Shift 加 Enter 换行。</span>
    </div>
    {name === "" && <p id="nickname-fallback-help" className={styles.hint}>保存后将显示登录账号</p>}
    {error && <p className={styles.error} role="alert">{error}</p>}
  </form>;
}
