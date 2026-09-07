"use client";

import { useId, useRef, type ReactNode } from "react";
import { XIcon } from "@phosphor-icons/react";
import { useDialogFocus } from "../components/useDialogFocus";
import styles from "./ContentDialog.module.css";

export default function ContentDialog({ title, subtitle, children, footer, status, busy = false, onClose }: {
  title: string;
  subtitle?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  status?: { error: string; message: string };
  busy?: boolean;
  onClose: () => void;
}) {
  const titleId = useId();
  const panelRef = useRef<HTMLElement | null>(null);
  useDialogFocus(true, panelRef, { onClose, busy });

  return <div className={`modal-backdrop ${styles.backdrop}`} onClick={(event) => {
    if (event.target === event.currentTarget && !busy) onClose();
  }}>
    <section ref={panelRef} className={`modal-panel ${styles.panel}`} role="dialog" aria-modal="true" aria-labelledby={titleId} tabIndex={-1}>
      <header className={styles.header}>
        <div className={styles.heading}><h2 id={titleId}>{title}</h2>{subtitle && <div className={styles.subtitle}>{subtitle}</div>}</div>
        <button type="button" className={styles.close} aria-label={`关闭${title}`} disabled={busy} onClick={onClose}><XIcon size={20} aria-hidden="true" /></button>
      </header>
      <div className={styles.body}>{children}</div>
      {footer && <footer className={styles.footer}>
        {(status?.error || status?.message) && <p className={styles.status} data-error={Boolean(status.error)} role={status.error ? "alert" : "status"}>{status.error || status.message}</p>}
        {footer}
      </footer>}
    </section>
  </div>;
}
