"use client";

import { useEffect, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { canAccessAccounts } from "../lib/accountAccess";
import { publicAssetPath } from "../lib/paths";
import { sessionQueryOptions } from "../lib/queries";
import AppShell from "./AppShell";
import { Loading, Notice } from "./Feedback";

export default function AccountPageAccess({ children }: { children: ReactNode }) {
  const session = useQuery(sessionQueryOptions());
  const allowed = canAccessAccounts(session.data?.role);

  useEffect(() => {
    if (session.data && !allowed) window.location.replace(publicAssetPath("/overview"));
  }, [allowed, session.data]);

  if (!allowed) {
    return <AppShell active="accounts">
      {session.isError
        ? <Notice tone="error">权限信息读取失败，请刷新页面重试。</Notice>
        : <Loading label="正在确认访问权限" />}
    </AppShell>;
  }

  return children;
}
