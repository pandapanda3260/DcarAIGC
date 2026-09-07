"use client";

import AppShell from "./components/AppShell";
import { ReadErrorState } from "./components/Feedback";
import { useWorkbench } from "./components/WorkbenchContext";

export default function ErrorPage({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  const { activeSection } = useWorkbench();
  const content = <ReadErrorState title="页面暂时无法打开" description="请重新加载，或从侧栏进入其他页面。" retrying={false} onRetry={reset} />;
  return activeSection ? <AppShell active={activeSection}>{content}</AppShell> : <main className="main-area">{content}</main>;
}
