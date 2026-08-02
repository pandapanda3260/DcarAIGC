import Link from "next/link";
import type { ReactNode } from "react";
import type { Section } from "../lib/types";

const navItems: Array<{ id: Section; label: string; mark: string; href: string }> = [
  { id: "overview", label: "概览", mark: "概", href: "/overview" },
  { id: "tasks", label: "任务", mark: "任", href: "/tasks" },
  { id: "accounts", label: "账号", mark: "账", href: "/accounts" },
  { id: "contents", label: "内容", mark: "内", href: "/contents" },
  { id: "selling-points", label: "卖点", mark: "卖", href: "/selling-points" },
];

const pageCopy: Record<Section, { eyebrow: string; title: string }> = {
  overview: { eyebrow: "OPERATIONS OVERVIEW", title: "内容运营概览" },
  tasks: { eyebrow: "REPORT TASKS", title: "数据报告任务" },
  accounts: { eyebrow: "OPERATED ACCOUNTS", title: "运营账号" },
  contents: { eyebrow: "CONTENT LIBRARY", title: "内容数据" },
  "selling-points": { eyebrow: "SELLING POINT STANDARD", title: "卖点标准" },
};

export default function AppShell({ active, actions, children }: { active: Section; actions?: ReactNode; children: ReactNode }) {
  const copy = pageCopy[active];
  return (
    <div className="app-shell insight-shell">
      <aside className="sidebar">
        <div className="brand"><div className="brand-mark">D</div><div><strong>DCar Insight</strong><span>内容运营工作台 · v8</span></div></div>
        <nav aria-label="主导航">
          <p>工作台</p>
          {navItems.map((item) => (
            <Link key={item.id} href={item.href} className={active === item.id ? "active" : ""} aria-current={active === item.id ? "page" : undefined}>
              <span>{item.mark}</span>{item.label}
            </Link>
          ))}
        </nav>
        <div className="sidebar-foot"><i className="live-dot online" /><div><strong>本地数据模式</strong><span>供应商调用受预算与幂等槽保护</span></div></div>
      </aside>
      <main className="main-area">
        <header className="topbar">
          <div><span className="eyebrow">{copy.eyebrow}</span><h1>{copy.title}</h1></div>
          <div className="topbar-actions"><span className="rule-chip">v8.2 合同</span><span className="safe-chip">本地优先</span>{actions}</div>
        </header>
        {children}
      </main>
    </div>
  );
}
