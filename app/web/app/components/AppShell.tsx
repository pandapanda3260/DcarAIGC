"use client";

import { useEffect, useSyncExternalStore, type ReactNode } from "react";
import type { Section } from "../lib/types";
import { useWorkbench } from "./WorkbenchContext";
import serviceStyles from "./DataServiceStatus.module.css";

const SERVICE_BANNER_STORAGE_KEY = "dcar.service-banner.v1";
type ServiceBannerRecord = { signature: string; dismissed: boolean };
let serviceBannerSnapshot: string | null | undefined;
const serviceBannerListeners = new Set<() => void>();

function readServiceBannerSnapshot() {
  if (typeof window === "undefined") return null;
  if (serviceBannerSnapshot === undefined) {
    try { serviceBannerSnapshot = window.sessionStorage.getItem(SERVICE_BANNER_STORAGE_KEY); }
    catch { serviceBannerSnapshot = null; }
  }
  return serviceBannerSnapshot;
}

function serviceBannerRecord(snapshot: string | null): ServiceBannerRecord | null {
  if (!snapshot) return null;
  try {
    const value: unknown = JSON.parse(snapshot);
    if (value && typeof value === "object" && "signature" in value && typeof value.signature === "string"
      && "dismissed" in value && typeof value.dismissed === "boolean") return value as ServiceBannerRecord;
  } catch { /* Ignore an unavailable or outdated browser record. */ }
  return null;
}

function writeServiceBannerRecord(record: ServiceBannerRecord) {
  serviceBannerSnapshot = JSON.stringify(record);
  try { window.sessionStorage.setItem(SERVICE_BANNER_STORAGE_KEY, serviceBannerSnapshot); }
  catch { /* In-memory dismissal still works when browser storage is unavailable. */ }
  for (const listener of serviceBannerListeners) listener();
}

function subscribeServiceBanner(listener: () => void) {
  serviceBannerListeners.add(listener);
  return () => { serviceBannerListeners.delete(listener); };
}

function serviceBannerServerSnapshot() { return null; }

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
    title: "SPU人群（未生效）",
    description: "维护车型、人群与场景的识别规则，并按统计窗口查看三者的数据表现。",
  },
  users: { eyebrow: "用户管理&质检", title: "用户权限", description: "查看工作台用户的注册信息与权限等级，修改资料或删除用户。" },
};

export default function AppShell({ active, actions, header, children }: { active: Section; actions?: ReactNode; header?: ReactNode; children: ReactNode }) {
  const copy = pageCopy[active];
  const { serviceState } = useWorkbench();
  const bannerSnapshot = useSyncExternalStore(subscribeServiceBanner, readServiceBannerSnapshot, serviceBannerServerSnapshot);
  // Loading during route changes is not a recovery. A real state or message
  // change starts a new notification, including an error after recovery.
  const bannerSignature = serviceState.kind === null ? null
    : JSON.stringify([serviceState.kind, serviceState.label, serviceState.description]);
  useEffect(() => {
    if (bannerSignature && serviceBannerRecord(readServiceBannerSnapshot())?.signature !== bannerSignature) {
      writeServiceBannerRecord({ signature: bannerSignature, dismissed: false });
    }
  }, [bannerSignature]);
  const bannerRecord = serviceBannerRecord(bannerSnapshot);
  const bannerDismissed = bannerRecord?.signature === bannerSignature && bannerRecord.dismissed;
  return (
      <main className="main-area" data-section={active} id="main-content" tabIndex={-1}>
        {header ?? (["contents", "accounts", "tasks"].includes(active) ? <h1 className="visually-hidden">{copy.title}</h1> : <header className="page-header" data-section={active}>
          <div className="page-header-copy"><span className="page-header-eyebrow">{copy.eyebrow}</span><h1 className="page-header-title">{copy.title}</h1>{copy.description && <p className="page-header-description">{copy.description}</p>}</div>
          {actions && <div className="page-header-actions">{actions}</div>}
        </header>)}
        {serviceState.kind === "error" && <div className={`${serviceStyles.banner} ${serviceStyles.offline}`} style={{ paddingInlineEnd: 60 }} role="status" hidden={bannerDismissed}>
          <strong>{serviceState.label}</strong>
          <span>{serviceState.description}</span>
          <button type="button" className={serviceStyles.dismiss} aria-label="关闭提示" onClick={() => {
            if (bannerSignature) writeServiceBannerRecord({ signature: bannerSignature, dismissed: true });
          }}>×</button>
        </div>}
        {children}
      </main>
  );
}
