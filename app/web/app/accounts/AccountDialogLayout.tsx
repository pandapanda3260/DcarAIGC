"use client";

import Image from "next/image";
import { useRef, type ReactNode } from "react";
import { PLATFORM_LOGO_PATHS } from "../lib/contentMedia";
import { label } from "../lib/format";
import { publicAssetPath } from "../lib/paths";
import type { Account } from "../lib/types";
import { useDialogFocus } from "../lib/useDialogFocus";
import styles from "./AccountDialogLayout.module.css";

export function AccountDialogIcon({ name }: { name: "close" | "lock" | "shield" | "chevron" }) {
  return <svg aria-hidden="true" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    {name === "close" && <path d="m5 5 10 10M15 5 5 15" />}
    {name === "chevron" && <path d="m5 7.5 5 5 5-5" />}
    {name === "lock" && <><rect x="5" y="9" width="10" height="8" rx="1.5" /><path d="M7 9V6a3 3 0 0 1 6 0v3M10 12v2" /></>}
    {name === "shield" && <><path d="m10 2 7 3v5c0 4-7 8-7 8s-7-4-7-8V5l7-3Z" /><path d="m7 10 2 2 4-4" /></>}
  </svg>;
}

export function AccountDialogIdentity({ account }: { account: Account }) {
  const identity = account.platforms[0];
  const platform = identity?.platform || account.directory_platform;
  const logo = platform && PLATFORM_LOGO_PATHS[platform];
  const nickname = identity?.nickname || "未命名账号";
  const uid = identity?.uid || account.directory_uid;
  return <div className={styles.identity}>
    <span className={styles.avatar}>{logo ? <Image src={publicAssetPath(logo)} alt="" width={24} height={24} unoptimized /> : nickname.slice(0, 1)}</span>
    <div className={styles.identityText}>
      <span className={styles.identityLabel}>{platform ? label(platform) : "平台账号"}</span>
      <div className={styles.identityName}>{nickname}</div>
    </div>
    {uid && <span className={styles.identityUid} title={`UID ${uid}`}>UID <span>{uid}</span></span>}
  </div>;
}

type AccountDialogLayoutProps = {
  id: string; title: string; description: string; onClose: () => void;
  busy?: boolean; wide?: boolean; initialFocus?: string; ariaLabel?: string;
  footer?: ReactNode; children: ReactNode;
};

export default function AccountDialogLayout({ id, title, description, onClose, busy = false, wide = false, initialFocus, ariaLabel, footer, children }: AccountDialogLayoutProps) {
  const dialogRef = useRef<HTMLElement>(null);
  const close = () => { if (!busy) onClose(); };
  useDialogFocus(true, dialogRef, close, initialFocus ?? `#${id}-close`);
  return <div className={`modal-backdrop ${styles.backdrop}`} role="presentation">
    <section ref={dialogRef} className={`modal-panel ${styles.panel} ${wide ? styles.wide : ""}`} role="dialog" aria-modal="true" aria-label={ariaLabel} aria-labelledby={ariaLabel ? undefined : `${id}-title`} aria-describedby={`${id}-description`} aria-busy={busy || undefined} tabIndex={-1}>
      <header className={styles.header}>
        <div><h3 id={`${id}-title`}>{title}</h3><p id={`${id}-description`}>{description}</p></div>
        <button id={`${id}-close`} type="button" className={styles.iconButton} onClick={close} disabled={busy} aria-label={`关闭${title}`}><AccountDialogIcon name="close" /></button>
      </header>
      <div className={styles.body}>{children}</div>
      <footer className={styles.footer}>{footer ?? <>
        <span className={styles.footerHint}><AccountDialogIcon name="lock" />只读资料</span>
        <button type="button" className={styles.primaryButton} disabled={busy} onClick={close}>关闭</button>
      </>}</footer>
    </section>
  </div>;
}
