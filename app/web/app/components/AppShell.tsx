import Link from "next/link";
import Image from "next/image";
import type { ReactNode } from "react";
import type { Section } from "../lib/types";

const navItems: Array<{ id: Section; label: string; href: string }> = [
  { id: "overview", label: "概览", href: "/overview" },
  { id: "tasks", label: "任务", href: "/tasks" },
  { id: "accounts", label: "账号", href: "/accounts" },
  { id: "contents", label: "内容", href: "/contents" },
  { id: "selling-points", label: "卖点", href: "/selling-points" },
];

const navIconShapes: Record<Section, ReactNode> = {
  overview: <><rect x="3" y="3" width="6" height="6" rx="1.5" /><rect x="11" y="3" width="6" height="4" rx="1.5" /><rect x="3" y="11" width="6" height="6" rx="1.5" /><rect x="11" y="9" width="6" height="8" rx="1.5" /></>,
  tasks: <><path d="M6.5 4h-1A1.5 1.5 0 0 0 4 5.5v10A1.5 1.5 0 0 0 5.5 17h9a1.5 1.5 0 0 0 1.5-1.5v-10A1.5 1.5 0 0 0 14.5 4h-1" /><rect x="7" y="2.5" width="6" height="3" rx="1.2" /><path d="m7 11 2 2 4-4" /></>,
  accounts: <><circle cx="10" cy="7" r="3" /><path d="M4.5 17c.6-3.1 2.6-4.8 5.5-4.8s4.9 1.7 5.5 4.8" /></>,
  contents: <><path d="M5 2.75h6l4 4V16a1.5 1.5 0 0 1-1.5 1.5h-7A1.5 1.5 0 0 1 5 16V2.75Z" /><path d="M11 2.75V7h4M7.5 10.5h5M7.5 14h4" /></>,
  "selling-points": <><path d="M3.5 9.25V5.5a2 2 0 0 1 2-2h3.75l7.1 7.1a1.75 1.75 0 0 1 0 2.48l-3.27 3.27a1.75 1.75 0 0 1-2.48 0L3.5 9.25Z" /><circle cx="7" cy="7" r="1" /></>,
};

function NavIcon({ section }: { section: Section }) {
  return <svg className="nav-icon" data-nav-icon={section} viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false">{navIconShapes[section]}</svg>;
}

const pageCopy: Record<Section, { eyebrow: string; title: string; description?: string }> = {
  overview: { eyebrow: "OPERATIONS OVERVIEW", title: "数据概览", description: "多渠道内容运营核心指标总览与场景分析" },
  tasks: { eyebrow: "REPORT TASKS", title: "数据报告任务" },
  accounts: { eyebrow: "OPERATED ACCOUNTS", title: "运营账号" },
  contents: { eyebrow: "CONTENT LIBRARY", title: "内容数据" },
  "selling-points": {
    eyebrow: "SELLING POINT STANDARD",
    title: "卖点标准",
    description: "围绕 E、X、M 三个业务场景，提供清晰的标签定义与分级规则，为内容评估与运营复核提供统一规范。",
  },
};

export default function AppShell({ active, actions, children }: { active: Section; actions?: ReactNode; children: ReactNode }) {
  const copy = pageCopy[active];
  return (
    <div className="app-shell insight-shell">
      <aside className="sidebar">
        <div className="brand"><Image className="brand-mark" src="/dongchedi-app-icon.svg" alt="懂车帝 App" width={38} height={38} unoptimized /><div><strong>Dcar Sentinel</strong><span>内容运营工作台 · V1.0</span></div></div>
        <nav aria-label="主导航">
          <p>工作台</p>
          {navItems.map((item) => (
            <Link key={item.id} href={item.href} className={active === item.id ? "active" : ""} aria-current={active === item.id ? "page" : undefined}>
              <NavIcon section={item.id} />{item.label}
            </Link>
          ))}
        </nav>
        <div className="sidebar-foot"><i className="live-dot online" /><div><strong>本地数据模式</strong><span>供应商调用受预算与幂等槽保护</span></div></div>
      </aside>
      <main className="main-area" data-section={active}>
        <header className="topbar" data-section={active}>
          <div className="topbar-copy"><span className="eyebrow">{copy.eyebrow}</span><h1>{copy.title}</h1>{copy.description && <p>{copy.description}</p>}</div>
          {actions && <div className="topbar-actions">{actions}</div>}
        </header>
        {children}
      </main>
    </div>
  );
}
