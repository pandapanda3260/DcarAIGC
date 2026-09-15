"use client";

import { useState } from "react";
import { skipToken, useQuery } from "@tanstack/react-query";
import { dataFreshnessNotice, type ServiceHealth } from "../lib/serviceStatus";
import styles from "./DataFreshnessNote.module.css";

function FreshnessMessage({ message }: { message: string }) {
  const [dismissed, setDismissed] = useState(false);
  if (dismissed) return null;
  return <div className={styles.note} role="status" aria-live="polite" data-freshness-note>
    <svg className={styles.icon} viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth={1.4} strokeLinecap="round" aria-hidden="true"><circle cx="10" cy="10" r="7" /><path d="M10 6v4l2.5 1.5" /></svg>
    <span>{message}</span>
    <button type="button" className={styles.dismiss} aria-label="关闭数据时效提示" onClick={() => setDismissed(true)}><svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" aria-hidden="true"><path d="m6 6 8 8M14 6l-8 8" /></svg></button>
  </div>;
}

/** Show next to loaded data; observe the shell's health cache without another request. */
export default function DataFreshnessNote() {
  const health = useQuery<ServiceHealth>({ queryKey: ["system", "health"], queryFn: skipToken, enabled: false });
  const message = dataFreshnessNotice(health.data, health.isError);
  // A changed reason or recovery starts a new notice; routine polling keeps dismissal.
  return message ? <FreshnessMessage key={message} message={message} /> : null;
}
