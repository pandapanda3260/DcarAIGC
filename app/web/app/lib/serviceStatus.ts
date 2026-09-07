export type ServiceHealth = {
  status: string;
  read_only: boolean;
  automation?: {
    scheduler_state: "running" | "paused" | "stopped" | "read_only";
    report_from_date?: string | null;
    paid_dispatch_state?: string | null;
  };
  data_freshness?: {
    status: "current" | "stale" | "unknown";
    last_successful_capture_at?: string | null;
  };
  snapshot_sync?: {
    status: "current" | "delayed" | "unknown";
    last_verified_install_at: string | null;
    window_state: "active" | "inactive" | "unknown";
    next_scheduled_at?: string | null;
  } | null;
};
export type ServiceState = {
  // AppShell renders faults in red and other non-running conditions in amber.
  kind: "checking" | "offline" | "read-only" | "online" | "paused" | "stopped" | "unknown" | "stale";
  label: string;
  description: string;
};

function captureTime(health: ServiceHealth): string {
  const value = health.data_freshness?.last_successful_capture_at;
  // No parsable capture time means we simply say nothing; "unknown" text carries no action.
  if (!value || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)
    || !Number.isFinite(Date.parse(value))) return "";
  const formatted = new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  }).format(new Date(value));
  return `最近成功采集：${formatted}（北京时间）。`;
}

function snapshotTime(value: string | null | undefined): string {
  if (!value || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)
    || !validDay(value.slice(0, 10)) || !Number.isFinite(Date.parse(value))) return "";
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  }).format(new Date(value));
}

function snapshotStatus(health: ServiceHealth): ServiceState {
  const sync = health.snapshot_sync;
  const installed = snapshotTime(sync?.last_verified_install_at);
  const lastSync = installed ? `最近同步：${installed}（北京时间）。` : "";
  const next = snapshotTime(sync?.next_scheduled_at);
  const schedule = sync?.window_state === "inactive"
    ? `当前不在自动发布时段，00:00–09:00（北京时间）不累计同步延迟。${next ? `下次计划发布：${next}（北京时间）。` : ""}`
    : "";
  if (sync?.status === "current" && installed) {
    return { kind: "online", label: "快照同步正常", description: `${lastSync}线上展示已发布的只读数据，同步成功不代表采集完整。${schedule}` };
  }
  if (sync?.status === "delayed" && installed) {
    return { kind: "stale", label: "快照同步延迟", description: `${lastSync}已超过计划发布时段内的同步等待时间，请检查自动发布任务。${schedule}` };
  }
  return { kind: "unknown", label: "同步状态待确认", description: `${lastSync}暂时无法确认最近的快照同步状态。${schedule}` };
}

export function dataServiceStatus(health: ServiceHealth | undefined, failed: boolean): ServiceState {
  // A failed refresh must not keep claiming that stale cached health is good.
  if (failed || (health && (health.status !== "ok" || typeof health.read_only !== "boolean"))) {
    return { kind: "offline", label: "数据服务不可用", description: "暂时无法读取业务数据，请稍后刷新页面。登录与用户权限管理不受影响。" };
  }
  if (!health) return { kind: "checking", label: "正在检查数据服务", description: "" };
  const lastCapture = captureTime(health);
  const automation = health.automation;
  if (health.read_only || automation?.scheduler_state === "read_only") {
    return snapshotStatus(health);
  }
  // A read-only replica has no local capture duty. On the writer, a broken gate must not be
  // disguised as a planned scheduler pause or stop.
  if (automation?.paid_dispatch_state === "invalid") {
    return { kind: "offline", label: "自动抓取异常", description: "采集通道校验未通过，自动抓取无法更新数据，请联系技术处理。" };
  }
  if (automation?.scheduler_state === "paused") {
    return { kind: "paused", label: "自动任务已暂停", description: `自动抓取和自动报告暂未运行。${lastCapture}` };
  }
  if (automation?.scheduler_state === "stopped") {
    return { kind: "stopped", label: "自动任务未运行", description: `当前可以查看已有数据，自动抓取和自动报告尚未启动。${lastCapture}` };
  }
  if (automation?.scheduler_state !== "running") {
    return { kind: "unknown", label: "自动任务状态未知", description: `暂时无法确认自动抓取和自动报告是否运行。${lastCapture}` };
  }
  if (["draining", "sealed"].includes(automation.paid_dispatch_state ?? "")) {
    return { kind: "paused", label: "自动抓取已暂停", description: `采集尚未恢复，自动报告仅使用已有数据。${lastCapture}` };
  }
  if (automation.paid_dispatch_state !== "open") {
    return { kind: "unknown", label: "自动抓取状态待确认", description: "暂时无法确认自动抓取是否可用，请稍后刷新页面。" };
  }
  if (health.data_freshness?.status === "stale") {
    return { kind: "stale", label: "数据更新延迟", description: "自动任务已运行，数据仍未更新至预期时间。" };
  }
  // Green only claims the scheduler is running and the paid gate is open. Freshness that is not
  // yet sealed into a coverage receipt is a normal daily window, not an operator-actionable fault;
  // a genuinely outdated day still surfaces through the "stale" branch above.
  return { kind: "online", label: "自动任务运行中", description: lastCapture };
}

type ReportTask = { task_type: string; period_start: string; period_end: string; task_status: string };

function validDay(value: string | null | undefined): value is string {
  return Boolean(value && /^\d{4}-\d{2}-\d{2}$/.test(value)
    && Number.isFinite(Date.parse(value)) && new Date(value).toISOString().slice(0, 10) === value);
}

/** Compare only reports from the enabled business day, after their next-day due time. */
export function dailyReportStatus(tasks: ReportTask[], reportFrom: string | null | undefined, now = new Date()) {
  const daily = tasks.filter((task) => task.task_type === "daily" && task.period_start === task.period_end && validDay(task.period_end));
  const completed = daily.filter((task) => ["succeeded", "partial"].includes(task.task_status));
  const latestDate = completed.map((task) => task.period_end).sort().at(-1) ?? null;
  const shanghai = new Date(now.getTime() + 8 * 60 * 60 * 1000);
  const dueDay = new Date(shanghai.getTime() - (shanghai.getUTCHours() >= 8 ? 1 : 2) * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const expected = validDay(reportFrom) && dueDay >= reportFrom ? dueDay : null;
  const dueTask = expected ? daily.find((task) => task.period_end === expected) : undefined;
  const missingDate = expected && !dueTask ? expected : null;
  const failedDate = expected && dueTask && ["failed", "interrupted", "cancelled"].includes(dueTask.task_status) ? expected : null;
  return {
    latestDate, missingDate, failedDate,
    message: [
      latestDate ? `最新已生成日报：${latestDate}。` : "尚无已生成日报。",
      missingDate ? `当前列表尚无 ${missingDate} 日报。` : "",
      failedDate ? `${failedDate} 日报生成未完成，请查看任务详情。` : "",
    ].filter(Boolean).join(""),
  };
}
