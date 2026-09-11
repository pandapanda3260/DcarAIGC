"use client";

import Link from "next/link";
import Image from "next/image";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useMemo, useRef, useSyncExternalStore, type ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  accountSearchQueryOptions,
  activeSellingPointsQueryOptions,
  contentSearchQueryOptions,
  defaultAccountSearchRequest,
  defaultContentSearchRequest,
  overviewQueryOptions,
  sessionQueryOptions,
  spuAssetsQueryOptions,
  spuStatsQueryOptions,
  tasksListQueryOptions,
  usersQueryOptions,
} from "../lib/queries";
import type { Section } from "../lib/types";
import { publicAssetPath } from "../lib/paths";
import { canAccessAccounts } from "../lib/accountAccess";
import { readQueryJson } from "../lib/api";
import { dataServiceStatus, type ServiceHealth } from "../lib/serviceStatus";
import { createServiceRecovery } from "../lib/serviceRecovery";
import { WorkbenchContext, workbenchSection } from "./WorkbenchContext";
import { ToastViewport } from "./Feedback";
import LogoutButton from "./LogoutButton";
import BackToTop from "./BackToTop";
import RouteLoading from "./RouteLoading";
import { navigationFeedbackStore } from "../../scripts/navigation-feedback.mjs";
import serviceStyles from "./DataServiceStatus.module.css";

// Warm the destination's JS and CSS alongside its data, before RSC navigation
// discovers them. Keep these imports lazy so the initial page stays small.
const sectionModules: Record<Section, () => Promise<unknown>> = {
  overview: () => import("../overview/OverviewPage"),
  contents: () => import("../contents/ContentsPage"),
  accounts: () => import("../accounts/AccountsPage"),
  "selling-points": () => import("../selling-points/SellingPointsPage"),
  "spu-audience": () => import("../spu-audience/SpuAudiencePage"),
  tasks: () => import("../tasks/TasksPage"),
  users: () => import("../users/UsersPage"),
};

const navItems: Array<{ id: Section; label: string; href: string }> = [
  { id: "overview", label: "概览", href: "/overview" },
  { id: "contents", label: "内容", href: "/contents" },
  { id: "accounts", label: "账号", href: "/accounts" },
  { id: "selling-points", label: "卖点", href: "/selling-points" },
  { id: "spu-audience", label: "SPU人群（未生效）", href: "/spu-audience" },
  { id: "tasks", label: "任务", href: "/tasks" },
];

const navIconShapes: Record<Section, ReactNode> = {
  overview: <><rect x="3" y="3" width="6" height="6" rx="1.5" /><rect x="11" y="3" width="6" height="4" rx="1.5" /><rect x="3" y="11" width="6" height="6" rx="1.5" /><rect x="11" y="9" width="6" height="8" rx="1.5" /></>,
  tasks: <><path d="M6.5 4h-1A1.5 1.5 0 0 0 4 5.5v10A1.5 1.5 0 0 0 5.5 17h9a1.5 1.5 0 0 0 1.5-1.5v-10A1.5 1.5 0 0 0 14.5 4h-1" /><rect x="7" y="2.5" width="6" height="3" rx="1.2" /><path d="m7 11 2 2 4-4" /></>,
  accounts: <><circle cx="10" cy="7" r="3" /><path d="M4.5 17c.6-3.1 2.6-4.8 5.5-4.8s4.9 1.7 5.5 4.8" /></>,
  contents: <><path d="M5 2.75h6l4 4V16a1.5 1.5 0 0 1-1.5 1.5h-7A1.5 1.5 0 0 1 5 16V2.75Z" /><path d="M11 2.75V7h4M7.5 10.5h5M7.5 14h4" /></>,
  "selling-points": <><path d="M3.5 9.25V5.5a2 2 0 0 1 2-2h3.75l7.1 7.1a1.75 1.75 0 0 1 0 2.48l-3.27 3.27a1.75 1.75 0 0 1-2.48 0L3.5 9.25Z" /><circle cx="7" cy="7" r="1" /></>,
  "spu-audience": <><circle cx="7" cy="6.5" r="2.4" /><circle cx="13.5" cy="8" r="1.9" /><path d="M3.2 16.5c.5-2.7 2-4.1 3.8-4.1s3.3 1.4 3.8 4.1" /><path d="M12.4 15.2c.4-1.9 1.5-3 2.9-3 .9 0 1.7.5 2.2 1.4" /></>,
  users: <><path d="M10 2.75 4 5v4.6c0 3.6 2.5 6.4 6 7.65 3.5-1.25 6-4.05 6-7.65V5l-6-2.25Z" /><path d="m7.4 10 1.8 1.8 3.4-3.6" /></>,
};

function NavIcon({ section }: { section: Section }) {
  return <svg className="nav-icon" data-nav-icon={section} viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false">{navIconShapes[section]}</svg>;
}

function NavLabel({ section, label }: { section: Section; label: string }) {
  return <><NavIcon section={section} />{label}</>;
}

// 侧栏"用户管理&质检"分组只对管理员及以上显示；真正的拦截在网关（/users 页面 303、/auth/users* 403），
// 这里只是隐藏入口。bypass 模式的会话没有 role，按运营人员处理。
function canManageUsers(role: string | undefined) {
  return role === "admin" || role === "superadmin";
}

export default function WorkbenchChrome({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const active = workbenchSection(pathname);
  return active ? <ActiveWorkbenchChrome active={active} pathname={pathname ?? `/${active}`}>{children}</ActiveWorkbenchChrome> : children;
}

function ActiveWorkbenchChrome({ active, pathname, children }: { active: Section; pathname: string; children: ReactNode }) {
  // External-store updates remain urgent even when navigation starts inside a
  // React transition. Never retain the previous page behind a pending spinner.
  const pending = useSyncExternalStore(navigationFeedbackStore.subscribe, navigationFeedbackStore.getSnapshot, navigationFeedbackStore.getServerSnapshot);
  const displayedSection = pending?.section ?? active;
  const router = useRouter();
  const queryClient = useQueryClient();
  const session = useQuery(sessionQueryOptions());
  const serviceHealth = useQuery({
    queryKey: ["system", "health"],
    queryFn: () => readQueryJson<ServiceHealth>("/api/v8/health", undefined, 5_000),
    staleTime: 15_000,
    refetchInterval: 30_000,
    refetchOnWindowFocus: "always",
    retry: false,
  });
  const serviceState = useMemo(() => dataServiceStatus(serviceHealth.data, serviceHealth.isError), [serviceHealth.data, serviceHealth.isError]);
  const recoverServiceQueries = useMemo(() => createServiceRecovery(queryClient), [queryClient]);
  useEffect(() => {
    const health = serviceHealth.data;
    void recoverServiceQueries(serviceHealth.isError ? false : health
      ? health.status === "ok" && typeof health.read_only === "boolean"
      : null);
  }, [recoverServiceQueries, serviceHealth.data, serviceHealth.isError]);
  const context = useMemo(() => ({ activeSection: displayedSection, serviceState }), [displayedSection, serviceState]);
  const showUserManagement = canManageUsers(session.data?.role);
  const showAccounts = canAccessAccounts(session.data?.role);
  const prefetchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const recentPrefetch = useRef<Section | null>(null);
  const recentPrefetchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  function cancelScheduledPrefetch() {
    if (prefetchTimer.current !== null) clearTimeout(prefetchTimer.current);
    prefetchTimer.current = null;
  }

  useEffect(() => () => {
    cancelScheduledPrefetch();
    if (recentPrefetchTimer.current !== null) clearTimeout(recentPrefetchTimer.current);
    recentPrefetch.current = null;
    recentPrefetchTimer.current = null;
  }, []);

  useEffect(() => {
    // A route or permission change ends any intent captured by the previous navigation state.
    if (prefetchTimer.current !== null) clearTimeout(prefetchTimer.current);
    prefetchTimer.current = null;
  }, [pathname, showAccounts, showUserManagement]);

  function prefetchSection(section: Section) {
    if (pathname.replace(/\/+$/, "") === `/${section}`) return Promise.resolve();
    if (section === "accounts" && !showAccounts) return Promise.resolve();
    if (section === "users" && !showUserManagement) return Promise.resolve();
    if (recentPrefetch.current === section) return Promise.resolve();
    router.prefetch(`/${section}`);
    // A speculative module load must never block data loading or navigation.
    void sectionModules[section]().catch(() => {});
    recentPrefetch.current = section;
    if (recentPrefetchTimer.current !== null) clearTimeout(recentPrefetchTimer.current);
    recentPrefetchTimer.current = setTimeout(() => {
      recentPrefetch.current = null;
      recentPrefetchTimer.current = null;
    }, 1_000);
    switch (section) {
      case "overview":
        return queryClient.prefetchQuery(overviewQueryOptions());
      case "contents":
        return queryClient.prefetchQuery(contentSearchQueryOptions(defaultContentSearchRequest));
      case "accounts":
        if (!showAccounts) return;
        return queryClient.prefetchQuery(accountSearchQueryOptions(defaultAccountSearchRequest));
      case "selling-points":
        return queryClient.prefetchQuery(activeSellingPointsQueryOptions());
      case "spu-audience":
        return Promise.all([
          queryClient.prefetchQuery(spuAssetsQueryOptions()),
          queryClient.prefetchQuery(spuStatsQueryOptions("last_week", "")),
        ]);
      case "tasks":
        return queryClient.prefetchQuery(tasksListQueryOptions());
      case "users":
        return queryClient.prefetchQuery(usersQueryOptions());
    }
  }

  function schedulePrefetch(section: Section) {
    cancelScheduledPrefetch();
    prefetchTimer.current = setTimeout(() => {
      prefetchTimer.current = null;
      void prefetchSection(section);
    }, 120);
  }

  return (
    <WorkbenchContext.Provider value={context}>
    <div className="app-shell insight-shell">
      <aside className="sidebar">
        <div className="brand"><Image className="brand-mark" src={publicAssetPath("/dongchedi-app-icon.svg")} alt="懂车帝 App" width={38} height={38} unoptimized /><div><strong>Dcar AIGC</strong><span>开心瓦瓦·运营工作台</span></div></div>
        <nav aria-label="主导航">
          <p>AIGC数据统计</p>
          {navItems.filter((item) => item.id !== "accounts" || showAccounts).map((item) => (
            <Link
              key={item.id}
              href={item.href}
              prefetch={false}
              className={displayedSection === item.id ? "active" : ""}
              aria-current={displayedSection === item.id ? "page" : undefined}
              onPointerEnter={() => schedulePrefetch(item.id)}
              onPointerLeave={cancelScheduledPrefetch}
              onClick={(event) => {
                cancelScheduledPrefetch();
                if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.altKey || event.shiftKey) return;
                void prefetchSection(item.id);
              }}
              onPointerDown={(event) => {
                if (event.button !== 0 || event.metaKey || event.ctrlKey || event.altKey || event.shiftKey) return;
                cancelScheduledPrefetch();
                void prefetchSection(item.id);
              }}
              onFocus={() => schedulePrefetch(item.id)}
              onBlur={cancelScheduledPrefetch}
            >
              <NavLabel section={item.id} label={item.label} />
            </Link>
          ))}
          {showUserManagement && <>
            <p>用户管理&质检</p>
            <Link
              href="/users"
              prefetch={false}
              className={displayedSection === "users" ? "active" : ""}
              aria-current={displayedSection === "users" ? "page" : undefined}
              onPointerEnter={() => schedulePrefetch("users")}
              onPointerLeave={cancelScheduledPrefetch}
              onClick={(event) => {
                cancelScheduledPrefetch();
                if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.altKey || event.shiftKey) return;
                void prefetchSection("users");
              }}
              onPointerDown={(event) => {
                if (event.button !== 0 || event.metaKey || event.ctrlKey || event.altKey || event.shiftKey) return;
                cancelScheduledPrefetch();
                void prefetchSection("users");
              }}
              onFocus={() => schedulePrefetch("users")}
              onBlur={cancelScheduledPrefetch}
            >
              <NavLabel section="users" label="用户权限" />
            </Link>
          </>}
        </nav>
        <div className="sidebar-foot">
          <i className={serviceStyles.indicator} data-state={serviceState.kind ?? undefined} aria-hidden="true" />
          <div role="status" aria-live="polite" aria-busy={serviceState.kind === null} title={serviceState.description || undefined} aria-label={[serviceState.label, serviceState.description].filter(Boolean).join("。") || "正在读取系统状态"}>
            <strong className={serviceStyles.footerLabel}>{serviceState.label || <span className={serviceStyles.loadingLabel} aria-hidden="true" />}</strong>
          </div>
          <LogoutButton />
        </div>
      </aside>
      {pending ? <RouteLoading section={pending.section} /> : children}
      <BackToTop pageKey={pending?.href ?? pathname} />
      <ToastViewport />
    </div>
    </WorkbenchContext.Provider>
  );
}
