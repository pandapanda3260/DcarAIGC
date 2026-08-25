import Link from "next/link";
import Image from "next/image";
import type { ReactNode } from "react";
import type { Section } from "../lib/types";
import { publicAssetPath } from "../lib/paths";
import { ToastViewport } from "./Feedback";
import LogoutButton from "./LogoutButton";

const navItems: Array<{ id: Section; label: string; href: string }> = [
  { id: "overview", label: "概览", href: "/overview" },
  { id: "contents", label: "内容", href: "/contents" },
  { id: "accounts", label: "账号", href: "/accounts" },
  { id: "selling-points", label: "卖点", href: "/selling-points" },
  { id: "spu-audience", label: "SPU人群", href: "/spu-audience" },
  { id: "tasks", label: "任务", href: "/tasks" },
];

const navIconShapes: Record<Section, ReactNode> = {
  overview: <><rect x="3" y="3" width="6" height="6" rx="1.5" /><rect x="11" y="3" width="6" height="4" rx="1.5" /><rect x="3" y="11" width="6" height="6" rx="1.5" /><rect x="11" y="9" width="6" height="8" rx="1.5" /></>,
  tasks: <><path d="M6.5 4h-1A1.5 1.5 0 0 0 4 5.5v10A1.5 1.5 0 0 0 5.5 17h9a1.5 1.5 0 0 0 1.5-1.5v-10A1.5 1.5 0 0 0 14.5 4h-1" /><rect x="7" y="2.5" width="6" height="3" rx="1.2" /><path d="m7 11 2 2 4-4" /></>,
  accounts: <><circle cx="10" cy="7" r="3" /><path d="M4.5 17c.6-3.1 2.6-4.8 5.5-4.8s4.9 1.7 5.5 4.8" /></>,
  contents: <><path d="M5 2.75h6l4 4V16a1.5 1.5 0 0 1-1.5 1.5h-7A1.5 1.5 0 0 1 5 16V2.75Z" /><path d="M11 2.75V7h4M7.5 10.5h5M7.5 14h4" /></>,
  "selling-points": <><path d="M3.5 9.25V5.5a2 2 0 0 1 2-2h3.75l7.1 7.1a1.75 1.75 0 0 1 0 2.48l-3.27 3.27a1.75 1.75 0 0 1-2.48 0L3.5 9.25Z" /><circle cx="7" cy="7" r="1" /></>,
  "spu-audience": <><circle cx="7" cy="6.5" r="2.4" /><circle cx="13.5" cy="8" r="1.9" /><path d="M3.2 16.5c.5-2.7 2-4.1 3.8-4.1s3.3 1.4 3.8 4.1" /><path d="M12.4 15.2c.4-1.9 1.5-3 2.9-3 .9 0 1.7.5 2.2 1.4" /></>,
};

function NavIcon({ section }: { section: Section }) {
  return <svg className="nav-icon" data-nav-icon={section} viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false">{navIconShapes[section]}</svg>;
}

const pageCopy: Record<Section, { eyebrow: string; title: string; description?: string }> = {
  overview: { eyebrow: "全渠道运营", title: "数据概览", description: "多渠道内容运营核心指标总览与场景分析" },
  tasks: { eyebrow: "报告版本留档", title: "数据报告任务" },
  accounts: { eyebrow: "账号档案", title: "运营账号" },
  contents: { eyebrow: "内容库", title: "内容数据" },
  "selling-points": {
    eyebrow: "评估标准基线",
    title: "卖点标准",
    description: "围绕 E、X、M 三个业务场景，提供清晰的标签定义与分级规则，为内容评估与运营复核提供统一规范。",
  },
  "spu-audience": {
    eyebrow: "车型 × 人群 × 场景",
    title: "SPU人群",
    description: "维护车型、人群与场景的识别规则，并按统计窗口查看三者的数据表现。",
  },
};

export default function AppShell({ active, actions, children }: { active: Section; actions?: ReactNode; children: ReactNode }) {
  const copy = pageCopy[active];
  return (
    <div className="app-shell insight-shell">
      <aside className="sidebar">
        <div className="brand"><Image className="brand-mark" src={publicAssetPath("/dongchedi-app-icon.svg")} alt="懂车帝 App" width={38} height={38} unoptimized /><div><strong>Dcar Sentinel</strong><span>内容运营工作台 · V1.0</span></div></div>
        <nav aria-label="主导航">
          <p>AIGC数据统计</p>
          {navItems.map((item) => (
            <Link key={item.id} href={item.href} className={active === item.id ? "active" : ""} aria-current={active === item.id ? "page" : undefined}>
              <NavIcon section={item.id} />{item.label}
            </Link>
          ))}
        </nav>
        <div className="sidebar-foot"><i className="live-dot online" /><div><strong>数据服务正常</strong></div><LogoutButton /></div>
      </aside>
      <main className="main-area" data-section={active}>
        {["contents", "accounts", "tasks"].includes(active) ? <h1 className="visually-hidden">{copy.title}</h1> : <header className="topbar" data-section={active}>
          <div className="topbar-copy"><span className="eyebrow">{copy.eyebrow}</span><h1>{copy.title}</h1>{copy.description && <p>{copy.description}</p>}</div>
          {actions && <div className="topbar-actions">{actions}</div>}
        </header>}
        {children}
      </main>
      <ToastViewport />
    </div>
  );
}
