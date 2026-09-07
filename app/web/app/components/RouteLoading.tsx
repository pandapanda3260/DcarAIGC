"use client";

import { useEffect, useState } from "react";
import AppShell from "./AppShell";
import { Loading } from "./Feedback";
import { useWorkbench } from "./WorkbenchContext";
import type { Section } from "../lib/types";
import styles from "./RouteLoading.module.css";

const destinationCopy: Record<Section, { eyebrow: string; title: string }> = {
  overview: { eyebrow: "全渠道运营", title: "数据概览" },
  contents: { eyebrow: "内容资料库", title: "发布内容明细" },
  accounts: { eyebrow: "账号档案", title: "账号信息" },
  "selling-points": { eyebrow: "评估标准基线", title: "卖点标准" },
  "spu-audience": { eyebrow: "车型 × 人群 × 场景", title: "SPU人群（未生效）" },
  tasks: { eyebrow: "每次生成都会保留", title: "日报、周报与自定义报告" },
  users: { eyebrow: "用户管理&质检", title: "用户权限" },
};

function DestinationSkeleton({ section }: { section: Section }) {
  const copy = destinationCopy[section];
  const header = <header className="page-header" data-section={section}>
    <div className="page-header-copy"><span className="page-header-eyebrow">{copy.eyebrow}</span><h1 className="page-header-title">{copy.title}</h1></div>
  </header>;
  return <AppShell active={section} header={header}>
    <section className={`page-stack wide-stack ${styles.skeleton}`} data-navigation-pending={section} aria-busy="true" aria-label={`正在打开${copy.title}`}>
      <span className="visually-hidden" role="status">正在打开{copy.title}</span>
      <div className={styles.toolbar} aria-hidden="true"><span /><span /><span /></div>
      <div className={styles.panel} aria-hidden="true">{[0, 1, 2, 3, 4].map((row) => <div key={row} className={styles.row}><span /><span /><span /></div>)}</div>
    </section>
  </AppShell>;
}

export default function RouteLoading({ section }: { section?: Section } = {}) {
  return section ? <DestinationSkeleton section={section} /> : <DeferredRouteLoading />;
}

function DeferredRouteLoading() {
  const { activeSection } = useWorkbench();
  const [visible, setVisible] = useState(false);
  useEffect(() => {
    const timer = setTimeout(() => setVisible(true), 150);
    return () => clearTimeout(timer);
  }, []);
  const loading = visible ? <Loading label="正在打开页面" /> : <div className="loading-screen" aria-hidden="true" />;
  return activeSection ? <AppShell active={activeSection}>{loading}</AppShell> : <main className="main-area">{loading}</main>;
}
