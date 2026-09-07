"use client";

import { useEffect, useState } from "react";
import AppShell from "./AppShell";
import { Loading } from "./Feedback";
import { useWorkbench } from "./WorkbenchContext";

export default function RouteLoading() {
  const { activeSection } = useWorkbench();
  const [visible, setVisible] = useState(false);
  useEffect(() => {
    const timer = setTimeout(() => setVisible(true), 150);
    return () => clearTimeout(timer);
  }, []);
  const loading = visible ? <Loading label="正在打开页面" /> : <div className="loading-screen" aria-hidden="true" />;
  return activeSection ? <AppShell active={activeSection}>{loading}</AppShell> : <main className="main-area">{loading}</main>;
}
