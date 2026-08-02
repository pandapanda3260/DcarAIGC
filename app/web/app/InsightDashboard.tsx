"use client";

import { useEffect, useMemo, useState } from "react";

type View = "overview" | "tasks" | "accounts" | "contents" | "selling-points";
type WindowKey = "yesterday" | "this_week" | "last_week";

type Metric = {
  kind: "quantity" | "ratio" | "score";
  value?: number | null;
  percentage?: number | null;
  unit: string;
  status: string;
  coverage_percentage?: number | null;
  reason: string;
};

type OverviewWindow = {
  period_start: string;
  period_end: string;
  eligible_count: number;
  unassociated_content_count: number;
  metrics: Record<string, Metric>;
  empty_explanation: string;
};

type Overview = {
  status: string;
  report_version: string;
  generated_at: string;
  timezone: string;
  windows: Record<WindowKey, OverviewWindow>;
  data_quality: {
    missing_published_at: number;
    pending_reviews: number;
    terminal_reviews: number;
  };
};

type Task = {
  id: string;
  name: string;
  task_type: string;
  period_start: string;
  period_end: string;
  task_status: string;
  progress: number;
  content_count: number;
  revision_count: number;
  message?: string;
};

type TaskDetail = Task & {
  events: Array<{ id: number; event_type: string; message: string; created_at: string }>;
  revisions: Array<{
    revision: number;
    created_at: string;
    files: Array<{ file_kind: string; byte_size: number; status: string }>;
  }>;
  content_counts: Record<string, number>;
};

type ReportView = {
  task: { task_status: string; name: string };
  data_quality: Record<string, number>;
  summary_metrics: Record<string, Metric>;
  platform_dimensions: Array<Record<string, string | number | null>>;
  account_type_dimensions: Array<Record<string, string | number | null>>;
  content_direction_dimensions: Array<Record<string, string | number | null>>;
  content_details: Array<Record<string, string | number | boolean | null>>;
  review_summary: Array<Record<string, string | number>>;
  capture_summary: Array<Record<string, string | number>>;
  provider_costs: Array<Record<string, string | number>>;
};

type Account = {
  id: number;
  phone: string;
  operator_name: string;
  account_type: string;
  content_direction: string;
  enabled: boolean;
  platforms: Array<{
    platform: string;
    uid: string;
    nickname: string;
    real_name_status: string;
  }>;
};

type ContentItem = {
  id: number;
  link_id: string;
  platform: string;
  canonical_url: string;
  platform_content_id: string | null;
  published_at: string | null;
  title: string;
  body: string;
  content_type: string;
  raw_account_uid: string;
  raw_account_name: string;
  account_type: string;
  content_direction: string;
  primary_selling_point_code: string | null;
  evidence_level: string | null;
  content_automotive_score: number | null;
  review_queue_id: number | null;
  review_status: string | null;
  pending_review_count: number;
  terminal_review_count: number;
  view_count: number | null;
  comment_count: number | null;
  metrics_captured_at: string | null;
  duplicate_original_link_id: string | null;
};

type AccountForm = {
  id: number | null;
  phone: string;
  operatorName: string;
  accountType: string;
  contentDirection: string;
  enabled: boolean;
  platforms: Record<string, { uid: string; nickname: string; realNameStatus: string }>;
};

type ContentForm = {
  id: number | null;
  platform: string;
  platformContentId: string;
  canonicalUrl: string;
  publishedAt: string;
  title: string;
  body: string;
  contentType: string;
  accountUid: string;
  accountName: string;
  accountType: string;
  contentDirection: string;
};

type SellingPoint = {
  code: string;
  tier: string;
  label: string;
  definition: string;
  scenes: string[];
  positive_evidence?: string[];
  negative_evidence?: string[];
  boundary_rules?: string[];
  enabled?: boolean;
  primary_hits?: number;
  total_hits?: number;
};

type SellingPointResponse = {
  taxonomy: { version: string; status: string } | null;
  items: SellingPoint[];
};

type ReviewForm = {
  decision: "confirm" | "override" | "insufficient_evidence" | "terminal_unavailable";
  reason: string;
  reviewer: string;
  evidenceType: string;
  evidenceText: string;
  primaryCode: string;
  sellingScore: string;
  automotiveScore: string;
  contentDirection: string;
};

type PointForm = {
  code: string;
  tier: "core" | "other";
  label: string;
  definition: string;
  positiveEvidence: string;
  negativeEvidence: string;
  boundaryRules: string;
  scenes: string[];
};

const emptyReviewForm: ReviewForm = {
  decision: "insufficient_evidence",
  reason: "",
  reviewer: "本地复核员",
  evidenceType: "review_note",
  evidenceText: "",
  primaryCode: "",
  sellingScore: "",
  automotiveScore: "",
  contentDirection: "unknown",
};

const emptyPointForm: PointForm = {
  code: "",
  tier: "other",
  label: "",
  definition: "",
  positiveEvidence: "",
  negativeEvidence: "",
  boundaryRules: "",
  scenes: [],
};

const platformKeys = ["douyin", "xiaohongshu", "wechat_channels", "kuaishou"];

function emptyAccountForm(): AccountForm {
  return {
    id: null, phone: "", operatorName: "", accountType: "unknown",
    contentDirection: "unknown", enabled: true,
    platforms: Object.fromEntries(platformKeys.map((platform) => [
      platform, { uid: "", nickname: "", realNameStatus: "unknown" },
    ])),
  };
}

const emptyContentForm: ContentForm = {
  id: null, platform: "douyin", platformContentId: "", canonicalUrl: "",
  publishedAt: "", title: "", body: "", contentType: "video",
  accountUid: "", accountName: "", accountType: "unknown", contentDirection: "unknown",
};

function parseCsv(text: string): Array<Record<string, string>> {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let quoted = false;
  const source = text.replace(/^\uFEFF/, "");
  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    if (quoted && character === '"' && source[index + 1] === '"') {
      cell += '"';
      index += 1;
    } else if (character === '"') {
      quoted = !quoted;
    } else if (character === "," && !quoted) {
      row.push(cell);
      cell = "";
    } else if ((character === "\n" || character === "\r") && !quoted) {
      if (character === "\r" && source[index + 1] === "\n") index += 1;
      row.push(cell);
      if (row.some((value) => value !== "")) rows.push(row);
      row = [];
      cell = "";
    } else {
      cell += character;
    }
  }
  row.push(cell);
  if (row.some((value) => value !== "")) rows.push(row);
  const headers = rows.shift()?.map((value) => value.trim()) ?? [];
  return rows.map((values) => Object.fromEntries(headers.map((header, index) => [header, values[index] ?? ""])));
}

const API_BASE = process.env.NEXT_PUBLIC_DCAR_API_BASE ?? "http://127.0.0.1:8765";

const navItems: Array<{ id: View; label: string; mark: string }> = [
  { id: "overview", label: "概览", mark: "概" },
  { id: "tasks", label: "任务", mark: "任" },
  { id: "accounts", label: "账号", mark: "账" },
  { id: "contents", label: "内容", mark: "内" },
  { id: "selling-points", label: "卖点", mark: "卖" },
];

const pageCopy: Record<View, { eyebrow: string; title: string }> = {
  overview: { eyebrow: "OPERATIONS OVERVIEW", title: "内容运营概览" },
  tasks: { eyebrow: "REPORT TASKS", title: "数据报告任务" },
  accounts: { eyebrow: "OPERATED ACCOUNTS", title: "运营账号" },
  contents: { eyebrow: "CONTENT LIBRARY", title: "内容数据" },
  "selling-points": { eyebrow: "SELLING POINT STANDARD", title: "卖点标准" },
};

const windowLabels: Record<WindowKey, string> = {
  yesterday: "昨天",
  this_week: "本周",
  last_week: "上周",
};

const metricLabels: Array<[string, string]> = [
  ["publication_count", "发布内容"],
  ["active_account_count", "发布账号"],
  ["view_count", "阅读 / 播放"],
  ["comment_count", "评论数"],
  ["verticality_rate", "内容垂直度"],
  ["selling_point_coverage_rate", "卖点覆盖率"],
  ["estimated_new_users", "预估拉新量"],
  ["estimated_reactivated_users", "预估拉活量"],
  ["estimated_leads", "预估线索量"],
];

const enumLabels: Record<string, string> = {
  douyin: "抖音",
  xiaohongshu: "小红书",
  wechat_channels: "视频号",
  kuaishou: "快手",
  boutique_ip: "精品 IP",
  original: "原创",
  mixed_edit: "混剪",
  unknown: "未知",
  new_car: "新车",
  used_car: "二手车",
  media: "媒体",
  other: "其他",
  yes: "是",
  no: "否",
  succeeded: "已完成",
  partial: "部分完成",
  failed: "失败",
  interrupted: "已中断",
  queued: "排队中",
  running: "运行中",
  daily: "日报",
  weekly: "周报",
  custom: "自定义",
};

function label(value: string | null | undefined) {
  if (!value) return "—";
  return enumLabels[value] ?? value;
}

function formatDate(value: string | null) {
  if (!value) return "缺失";
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(value));
}

function metricValue(metric: Metric | undefined) {
  if (!metric) return "—";
  if (metric.kind === "ratio") {
    return metric.percentage === null || metric.percentage === undefined
      ? "暂不可计算"
      : `${metric.percentage}%`;
  }
  if (metric.value === null || metric.value === undefined) return "暂不可计算";
  return new Intl.NumberFormat("zh-CN").format(metric.value);
}

function metricStatus(metric: Metric | undefined) {
  if (!metric) return "等待读取";
  const labels: Record<string, string> = {
    available: "可用",
    below_threshold: "覆盖不足",
    sample_only: "仅样本",
    not_applicable: "窗口无适用内容",
    not_calculable: "暂无模型",
    missing: "缺失",
    stale: "已过期",
  };
  return labels[metric.status] ?? metric.status;
}

async function readJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, init);
  if (!response.ok) {
    const body = (await response.json().catch(() => null)) as { detail?: string } | null;
    throw new Error(body?.detail || `${path} 返回 ${response.status}`);
  }
  return (await response.json()) as T;
}

export default function InsightDashboard() {
  const [view, setView] = useState<View>("overview");
  const [windowKey, setWindowKey] = useState<WindowKey>("yesterday");
  const [overview, setOverview] = useState<Overview | null>(null);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [taskModal, setTaskModal] = useState(false);
  const [taskForm, setTaskForm] = useState(() => {
    const yesterday = new Date(Date.now() - 86_400_000).toLocaleDateString("en-CA", { timeZone: "Asia/Shanghai" });
    return { periodStart: yesterday, periodEnd: yesterday, name: "" };
  });
  const [taskDetail, setTaskDetail] = useState<TaskDetail | null>(null);
  const [taskReport, setTaskReport] = useState<ReportView | null>(null);
  const [taskTab, setTaskTab] = useState("summary");
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [accountTotal, setAccountTotal] = useState(0);
  const [legacyUnassociated, setLegacyUnassociated] = useState(0);
  const [accountQuery, setAccountQuery] = useState("");
  const [accountTypeFilter, setAccountTypeFilter] = useState("");
  const [accountDirectionFilter, setAccountDirectionFilter] = useState("");
  const [accountPlatformFilter, setAccountPlatformFilter] = useState("");
  const [accountForm, setAccountForm] = useState<AccountForm | null>(null);
  const [contents, setContents] = useState<ContentItem[]>([]);
  const [contentTotal, setContentTotal] = useState(0);
  const [sellingPoints, setSellingPoints] = useState<SellingPointResponse>({ taxonomy: null, items: [] });
  const [contentQuery, setContentQuery] = useState("");
  const [contentPlatformFilter, setContentPlatformFilter] = useState("");
  const [contentTypeFilter, setContentTypeFilter] = useState("");
  const [contentDirectionFilter, setContentDirectionFilter] = useState("");
  const [contentForm, setContentForm] = useState<ContentForm | null>(null);
  const [reviewFilter, setReviewFilter] = useState("");
  const [reviewItem, setReviewItem] = useState<ContentItem | null>(null);
  const [reviewForm, setReviewForm] = useState<ReviewForm>(emptyReviewForm);
  const [draftMode, setDraftMode] = useState(false);
  const [pointForm, setPointForm] = useState<PointForm | null>(null);
  const [editingPointCode, setEditingPointCode] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [actionMessage, setActionMessage] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    const post = (body: Record<string, unknown>): RequestInit => ({
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    async function load() {
      try {
        const [overviewData, taskData, accountData, contentData, pointData] = await Promise.all([
          readJson<Overview>("/api/v8/overview", { signal: controller.signal }),
          readJson<{ items: Task[] }>("/api/v8/tasks", { signal: controller.signal }),
          readJson<{ items: Account[]; total: number; legacy_unassociated_content_count: number }>(
            "/api/v8/accounts/search",
            post({ page: 1, page_size: 50 }),
          ),
          readJson<{ items: ContentItem[]; total: number }>(
            "/api/v8/contents/search",
            post({ page: 1, page_size: 50 }),
          ),
          readJson<SellingPointResponse>("/api/v8/selling-points", { signal: controller.signal }),
        ]);
        setOverview(overviewData);
        setTasks(taskData.items);
        setAccounts(accountData.items);
        setAccountTotal(accountData.total);
        setLegacyUnassociated(accountData.legacy_unassociated_content_count);
        setContents(contentData.items);
        setContentTotal(contentData.total);
        setSellingPoints(pointData);
      } catch (reason) {
        if (!controller.signal.aborted) {
          setError(reason instanceof Error ? reason.message : "无法读取 v8 数据");
        }
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    }
    void load();
    return () => controller.abort();
  }, []);

  async function reloadAccounts(
    nextQuery = accountQuery,
    nextType = accountTypeFilter,
    nextDirection = accountDirectionFilter,
    nextPlatform = accountPlatformFilter,
  ) {
    const value = await readJson<{
      items: Account[]; total: number; legacy_unassociated_content_count: number;
    }>("/api/v8/accounts/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        page: 1, page_size: 50, query: nextQuery,
        account_type: nextType || null, content_direction: nextDirection || null,
        platform: nextPlatform || null,
      }),
    });
    setAccounts(value.items);
    setAccountTotal(value.total);
    setLegacyUnassociated(value.legacy_unassociated_content_count);
  }

  async function reloadContents(
    nextReviewStatus = reviewFilter,
    nextQuery = contentQuery,
    nextPlatform = contentPlatformFilter,
    nextType = contentTypeFilter,
    nextDirection = contentDirectionFilter,
  ) {
    const value = await readJson<{ items: ContentItem[]; total: number }>(
      "/api/v8/contents/search",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          page: 1,
          page_size: 50,
          query: nextQuery,
          review_status: nextReviewStatus || null,
          platform: nextPlatform || null,
          account_type: nextType || null,
          content_direction: nextDirection || null,
        }),
      },
    );
    setContents(value.items);
    setContentTotal(value.total);
  }

  function editAccount(account?: Account) {
    if (!account) {
      setAccountForm(emptyAccountForm());
      return;
    }
    const form = emptyAccountForm();
    form.id = account.id;
    form.phone = account.phone;
    form.operatorName = account.operator_name;
    form.accountType = account.account_type;
    form.contentDirection = account.content_direction;
    form.enabled = account.enabled;
    for (const identity of account.platforms) {
      form.platforms[identity.platform] = {
        uid: identity.uid, nickname: identity.nickname,
        realNameStatus: identity.real_name_status,
      };
    }
    setAccountForm(form);
  }

  async function saveAccount() {
    if (!accountForm) return;
    setSaving(true);
    setError("");
    setActionMessage("");
    try {
      const body = {
        phone: accountForm.phone, operator_name: accountForm.operatorName,
        account_type: accountForm.accountType,
        content_direction: accountForm.contentDirection, enabled: accountForm.enabled,
        platforms: platformKeys.filter((platform) => accountForm.platforms[platform].uid.trim()).map((platform) => ({
          platform, uid: accountForm.platforms[platform].uid,
          nickname: accountForm.platforms[platform].nickname,
          real_name_status: accountForm.platforms[platform].realNameStatus,
        })),
      };
      await readJson(accountForm.id ? `/api/v8/accounts/${accountForm.id}` : "/api/v8/accounts", {
        method: accountForm.id ? "PATCH" : "POST",
        headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
      });
      setAccountForm(null);
      await reloadAccounts();
      setActionMessage(accountForm.id ? "账号已更新" : "账号已新增");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "账号保存失败");
    } finally {
      setSaving(false);
    }
  }

  function editContent(item?: ContentItem) {
    setContentForm(item ? {
      id: item.id, platform: item.platform,
      platformContentId: item.platform_content_id ?? "", canonicalUrl: item.canonical_url,
      publishedAt: item.published_at?.slice(0, 16) ?? "", title: item.title,
      body: item.body ?? "", contentType: item.content_type ?? "unknown",
      accountUid: item.raw_account_uid ?? "", accountName: item.raw_account_name ?? "",
      accountType: item.account_type, contentDirection: item.content_direction,
    } : { ...emptyContentForm });
  }

  async function saveContent() {
    if (!contentForm) return;
    setSaving(true);
    setError("");
    setActionMessage("");
    try {
      const body = {
        platform: contentForm.platform,
        platform_content_id: contentForm.platformContentId || null,
        canonical_url: contentForm.canonicalUrl,
        published_at: contentForm.publishedAt ? new Date(contentForm.publishedAt).toISOString() : null,
        title: contentForm.title, body: contentForm.body, content_type: contentForm.contentType,
        account_uid: contentForm.accountUid, account_name: contentForm.accountName,
        account_type: contentForm.accountType, content_direction: contentForm.contentDirection,
      };
      await readJson(contentForm.id ? `/api/v8/contents/${contentForm.id}` : "/api/v8/contents", {
        method: contentForm.id ? "PATCH" : "POST",
        headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
      });
      setContentForm(null);
      await reloadContents();
      setActionMessage(contentForm.id ? "内容已更新" : "内容已新增");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "内容保存失败");
    } finally {
      setSaving(false);
    }
  }

  async function importCsv(file: File, entity: "account" | "content") {
    setSaving(true);
    setError("");
    setActionMessage("");
    try {
      const rows = parseCsv(await file.text());
      if (!rows.length) throw new Error("CSV 中没有可导入的数据行");
      const request = {
        source_name: file.name,
        rows,
      };
      if (entity === "content") {
        const validation = await readJson<{ total: number; valid: number; rejected: number }>(
          "/api/v8/contents/validate",
          { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(request) },
        );
        if (validation.rejected > 0) {
          throw new Error(`内容导入校验失败：${validation.rejected} / ${validation.total} 行无效，未写入数据库`);
        }
      }
      const path = entity === "account" ? "/api/v8/accounts/import" : "/api/v8/contents/import";
      const result = await readJson<{ inserted_rows: number; updated_rows: number; rejected_rows: number }>(path, {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(request),
      });
      if (entity === "account") await reloadAccounts(); else await reloadContents();
      setActionMessage(
        `${entity === "account" ? "账号" : "内容"}导入完成：新增 ${result.inserted_rows}，更新 ${result.updated_rows}，拒绝 ${result.rejected_rows}`,
      );
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "CSV 导入失败");
    } finally {
      setSaving(false);
    }
  }

  async function updateContentData(item: ContentItem) {
    setSaving(true);
    setError("");
    setActionMessage("");
    try {
      const result = await readJson<{ status: string; provider_cost: number }>(
        `/api/v8/contents/${item.id}/update-data`, { method: "POST" },
      );
      await reloadContents();
      setActionMessage(`${item.link_id} 数据更新${result.status === "succeeded" ? "完成" : "部分完成"}，本次供应商费用 $${result.provider_cost.toFixed(3)}`);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "内容数据更新失败");
    } finally {
      setSaving(false);
    }
  }

  async function reloadTasks() {
    const value = await readJson<{ items: Task[] }>("/api/v8/tasks");
    setTasks(value.items);
  }

  async function createReportTask() {
    setSaving(true);
    setError("");
    try {
      const detail = await readJson<TaskDetail>("/api/v8/tasks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          period_start: taskForm.periodStart,
          period_end: taskForm.periodEnd,
          name: taskForm.name || null,
        }),
      });
      setTaskModal(false);
      await reloadTasks();
      await openTask(detail.id);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "报告任务创建失败");
    } finally {
      setSaving(false);
    }
  }

  async function openTask(taskId: string) {
    setSaving(true);
    setError("");
    try {
      const detail = await readJson<TaskDetail>(`/api/v8/tasks/${taskId}`);
      setTaskDetail(detail);
      setTaskTab("summary");
      if (detail.revisions.length) {
        setTaskReport(await readJson<ReportView>(
          `/api/v8/tasks/${taskId}/revisions/${detail.revisions[0].revision}/report`,
        ));
      } else {
        setTaskReport(null);
      }
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "无法读取任务详情");
    } finally {
      setSaving(false);
    }
  }

  async function retryTask() {
    if (!taskDetail) return;
    setSaving(true);
    setError("");
    try {
      await readJson(`/api/v8/tasks/${taskDetail.id}/retry`, { method: "POST" });
      await reloadTasks();
      await openTask(taskDetail.id);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "任务重试失败");
    } finally {
      setSaving(false);
    }
  }

  async function openReview(item: ContentItem) {
    if (!item.review_queue_id) return;
    setSaving(true);
    setError("");
    try {
      if (item.review_status === "pending" || item.review_status === "manual_required") {
        await readJson(`/api/v8/reviews/${item.review_queue_id}/start`, { method: "POST" });
      }
      setReviewItem({ ...item, review_status: "in_review" });
      setReviewForm({
        ...emptyReviewForm,
        primaryCode: item.primary_selling_point_code ?? "",
        automotiveScore: item.content_automotive_score?.toString() ?? "",
        contentDirection: item.content_direction,
      });
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "无法开始复核");
    } finally {
      setSaving(false);
    }
  }

  async function submitReview() {
    if (!reviewItem?.review_queue_id) return;
    setSaving(true);
    setError("");
    try {
      const isOverride = reviewForm.decision === "override";
      await readJson(`/api/v8/reviews/${reviewItem.review_queue_id}/resolve`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          decision: reviewForm.decision,
          reason: reviewForm.reason,
          reviewer: reviewForm.reviewer,
          evidence_type: reviewForm.evidenceType,
          evidence_text: reviewForm.evidenceText,
          primary_selling_point_code: isOverride && reviewForm.primaryCode ? reviewForm.primaryCode : null,
          selling_point_score: isOverride && reviewForm.sellingScore ? Number(reviewForm.sellingScore) : null,
          selling_point_included: isOverride ? Boolean(reviewForm.primaryCode) : null,
          content_automotive_score: isOverride && reviewForm.automotiveScore ? Number(reviewForm.automotiveScore) : null,
          content_direction: isOverride ? reviewForm.contentDirection : null,
        }),
      });
      setReviewItem(null);
      await reloadContents();
      setOverview(await readJson<Overview>("/api/v8/overview"));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "复核提交失败");
    } finally {
      setSaving(false);
    }
  }

  async function beginTaxonomyEdit() {
    setSaving(true);
    setError("");
    try {
      await readJson("/api/v8/selling-points/draft", { method: "POST" });
      const draft = await readJson<SellingPointResponse>("/api/v8/selling-points/draft");
      setSellingPoints(draft);
      setDraftMode(true);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "无法创建卖点草稿");
    } finally {
      setSaving(false);
    }
  }

  function openPoint(point?: SellingPoint) {
    setEditingPointCode(point?.code ?? null);
    setPointForm(point ? {
      code: point.code,
      tier: point.tier === "core" ? "core" : "other",
      label: point.label,
      definition: point.definition,
      positiveEvidence: (point.positive_evidence ?? []).join("\n"),
      negativeEvidence: (point.negative_evidence ?? []).join("\n"),
      boundaryRules: (point.boundary_rules ?? []).join("\n"),
      scenes: point.scenes,
    } : emptyPointForm);
  }

  async function savePoint() {
    if (!pointForm) return;
    setSaving(true);
    setError("");
    const lines = (value: string) => value.split("\n").map((item) => item.trim()).filter(Boolean);
    const body = {
      code: pointForm.code,
      tier: pointForm.tier,
      label: pointForm.label,
      definition: pointForm.definition,
      positive_evidence: lines(pointForm.positiveEvidence),
      negative_evidence: lines(pointForm.negativeEvidence),
      boundary_rules: lines(pointForm.boundaryRules),
      scenes: pointForm.scenes,
    };
    try {
      await readJson(
        editingPointCode ? `/api/v8/selling-points/items/${editingPointCode}` : "/api/v8/selling-points/items",
        {
          method: editingPointCode ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      setSellingPoints(await readJson<SellingPointResponse>("/api/v8/selling-points/draft"));
      setPointForm(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "卖点保存失败");
    } finally {
      setSaving(false);
    }
  }

  async function removePoint(code: string) {
    setSaving(true);
    setError("");
    try {
      await readJson(`/api/v8/selling-points/items/${code}`, { method: "DELETE" });
      setSellingPoints(await readJson<SellingPointResponse>("/api/v8/selling-points/draft"));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "卖点删除失败");
    } finally {
      setSaving(false);
    }
  }

  async function publishTaxonomy() {
    setSaving(true);
    setError("");
    try {
      await readJson("/api/v8/selling-points/publish", { method: "POST" });
      setSellingPoints(await readJson<SellingPointResponse>("/api/v8/selling-points"));
      setDraftMode(false);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "卖点标准发布失败");
    } finally {
      setSaving(false);
    }
  }

  const activeWindow = overview?.windows[windowKey];
  const title = pageCopy[view];
  const pointFamilies = useMemo(() => {
    const families = new Map<string, number>();
    for (const point of sellingPoints.items) {
      const family = point.code.slice(0, 1);
      families.set(family, (families.get(family) ?? 0) + (point.primary_hits ?? 0));
    }
    return families;
  }, [sellingPoints]);

  return (
    <div className="app-shell insight-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">D</div>
          <div>
            <strong>DCar Insight</strong>
            <span>内容运营工作台 · v8</span>
          </div>
        </div>
        <nav aria-label="主导航">
          <p>工作台</p>
          {navItems.map((item) => (
            <button
              className={view === item.id ? "active" : ""}
              key={item.id}
              onClick={() => setView(item.id)}
              aria-current={view === item.id ? "page" : undefined}
            >
              <span aria-hidden="true">{item.mark}</span>
              {item.label}
            </button>
          ))}
        </nav>
        <div className="sidebar-foot">
          <i className={`live-dot ${overview && !error ? "online" : ""}`} />
          <div>
            <strong>{overview && !error ? "本地 API 已连接" : "正在连接本地 API"}</strong>
            <span>Asia/Shanghai · SQLite v3</span>
          </div>
        </div>
      </aside>

      <main className="main-area">
        <header className="topbar">
          <div>
            <span className="eyebrow">{title.eyebrow}</span>
            <h1>{title.title}</h1>
          </div>
          <div className="topbar-actions">
            <span className="rule-chip">报告合同 v8.0</span>
            <span className="safe-chip">本地运行</span>
          </div>
        </header>

        {loading && <div className="notice"><span>i</span>正在读取 v8 运营数据</div>}
        {error && <div className="notice error-notice"><span>!</span>读取失败：{error}</div>}
        {actionMessage && <div className="notice success-notice"><span>✓</span>{actionMessage}</div>}

        {view === "overview" && (
          <section className="page-stack">
            <div className="section-heading dashboard-heading">
              <div>
                <span className="eyebrow">LIVE DATABASE WINDOWS</span>
                <h2>按发布日期统计，不混用抓取时间</h2>
                <p>窗口采用 Asia/Shanghai；无适用内容时明确解释，不用 0 冒充结果。</p>
              </div>
              <div className="channel-switch" aria-label="统计窗口">
                {(Object.keys(windowLabels) as WindowKey[]).map((key) => (
                  <button
                    key={key}
                    className={windowKey === key ? "active" : ""}
                    onClick={() => setWindowKey(key)}
                  >
                    {windowLabels[key]}
                  </button>
                ))}
              </div>
            </div>

            <div className="metric-grid insight-metrics">
              {metricLabels.map(([key, metricLabel]) => {
                const metric = activeWindow?.metrics[key];
                return (
                  <article className="metric-card compact insight-metric" key={key}>
                    <div className="metric-card-head">
                      <span>{metricLabel}</span>
                      <span className={`metric-status ${metric?.status === "available" ? "available" : "limited"}`}>
                        {metricStatus(metric)}
                      </span>
                    </div>
                    <strong className="insight-metric-value">{metricValue(metric)}</strong>
                    <p>{metric?.reason || (metric?.coverage_percentage != null ? `数据覆盖 ${metric.coverage_percentage}%` : "以本窗口全部发布内容为分母")}</p>
                  </article>
                );
              })}
            </div>

            <div className="two-column">
              <article className="panel">
                <div className="panel-head">
                  <div>
                    <span className="eyebrow">WINDOW BOUNDARY</span>
                    <h3>{windowLabels[windowKey]}统计边界</h3>
                  </div>
                </div>
                <dl className="definition-list">
                  <div><dt>开始</dt><dd>{activeWindow ? formatDate(activeWindow.period_start) : "—"}</dd></div>
                  <div><dt>结束（不含）</dt><dd>{activeWindow ? formatDate(activeWindow.period_end) : "—"}</dd></div>
                  <div><dt>有效评估内容</dt><dd>{activeWindow?.eligible_count ?? "—"}</dd></div>
                  <div><dt>未关联账号内容</dt><dd>{activeWindow?.unassociated_content_count ?? "—"}</dd></div>
                </dl>
                {activeWindow?.empty_explanation && <p className="empty-explanation">{activeWindow.empty_explanation}</p>}
              </article>
              <article className="panel">
                <div className="panel-head">
                  <div>
                    <span className="eyebrow">DATA QUALITY</span>
                    <h3>迁移与复核状态</h3>
                  </div>
                </div>
                <div className="quality-grid">
                  <div><strong>{overview?.data_quality.missing_published_at ?? "—"}</strong><span>发布日期缺失</span></div>
                  <div><strong>{overview?.data_quality.pending_reviews ?? "—"}</strong><span>待复核 / 补证</span></div>
                  <div><strong>{overview?.data_quality.terminal_reviews ?? "—"}</strong><span>终态不可用</span></div>
                </div>
                <p className="panel-note">在内容页筛选并处理待复核记录；每次人工结论都会保留证据和不可变评估版本。</p>
              </article>
            </div>
          </section>
        )}

        {view === "tasks" && (
          <section className="page-stack">
            <div className="detail-toolbar">
              <div>
                <span className="eyebrow">DAILY · WEEKLY · CUSTOM</span>
                <h2>不可变报告任务</h2>
                <p>日报、周报和自定义区间统一使用 v8 合同，每次产物按 revision 留存。</p>
              </div>
              <button className="primary" disabled={saving} onClick={() => setTaskModal(true)}>新建任务</button>
            </div>
            <div className="panel table-panel">
              {tasks.length ? (
                <div className="table-scroll"><table><thead><tr><th>任务</th><th>类型</th><th>周期</th><th>状态</th><th>内容</th><th>版本</th><th>操作</th></tr></thead><tbody>
                  {tasks.map((task) => <tr key={task.id}><td><strong>{task.name}</strong><span>{task.id}</span></td><td>{label(task.task_type)}</td><td>{formatDate(task.period_start)} — {formatDate(task.period_end)}</td><td><span className="scene-tag">{label(task.task_status)} · {task.progress}%</span></td><td>{task.content_count}</td><td>{task.revision_count}</td><td><button className="text-button" disabled={saving} onClick={() => void openTask(task.id)}>查看详情</button></td></tr>)}
                </tbody></table></div>
              ) : <div className="empty-state"><strong>尚无 v8 报告任务</strong><span>调度器启用后会自动生成日报与周报；v7 历史报告保持只读。</span></div>}
            </div>
          </section>
        )}

        {view === "accounts" && (
          <section className="page-stack">
            <div className="detail-toolbar">
              <div>
                <span className="eyebrow">PHONE AS UNIQUE KEY</span>
                <h2>账号列表</h2>
                <p>手机号完整显示；搜索采用 POST body，不把手机号写入请求路径日志。</p>
              </div>
              <div className="placeholder-actions">
                <a className="secondary button-link" href={`${API_BASE}/api/v8/accounts/export`}>下载</a>
                <label className={`secondary button-link ${saving ? "disabled" : ""}`}>批量导入<input className="file-input" type="file" accept=".csv,text/csv" disabled={saving} onChange={(event) => { const file = event.target.files?.[0]; if (file) void importCsv(file, "account"); event.currentTarget.value = ""; }} /></label>
                <button className="primary" disabled={saving} onClick={() => editAccount()}>新增账号</button>
              </div>
            </div>
            <div className="filter-bar">
              <input aria-label="搜索账号" placeholder="搜索手机号、运营人员、UID 或昵称" value={accountQuery} onChange={(event) => setAccountQuery(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") void reloadAccounts(); }} />
              <select aria-label="账号类型" value={accountTypeFilter} onChange={(event) => { setAccountTypeFilter(event.target.value); void reloadAccounts(accountQuery, event.target.value, accountDirectionFilter, accountPlatformFilter); }}><option value="">全部账号类型</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option><option value="unknown">未知</option></select>
              <select aria-label="内容方向" value={accountDirectionFilter} onChange={(event) => { setAccountDirectionFilter(event.target.value); void reloadAccounts(accountQuery, accountTypeFilter, event.target.value, accountPlatformFilter); }}><option value="">全部内容方向</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option><option value="unknown">未知</option></select>
              <select aria-label="运营平台" value={accountPlatformFilter} onChange={(event) => { setAccountPlatformFilter(event.target.value); void reloadAccounts(accountQuery, accountTypeFilter, accountDirectionFilter, event.target.value); }}><option value="">全部平台</option>{platformKeys.map((platform) => <option key={platform} value={platform}>{label(platform)}</option>)}</select>
              <button className="secondary" onClick={() => void reloadAccounts()}>搜索</button>
              <span>当前显示 {accounts.length} / {accountTotal}</span>
            </div>
            <div className="notice inline-notice"><span>i</span>{legacyUnassociated} 条存量内容尚未关联账号；建库后按（平台、UID）回填，小红书缺 UID 的内容保留未关联口径。</div>
            <div className="panel table-panel">
              {accounts.length ? (
                <div className="table-scroll"><table><thead><tr><th>手机号 / 运营人员</th><th>账号类型</th><th>内容方向</th><th>平台身份</th><th>状态</th><th>操作</th></tr></thead><tbody>
                  {accounts.map((account) => <tr key={account.id}><td><strong>{account.phone}</strong><span>{account.operator_name || "未填写运营人员"}</span></td><td>{label(account.account_type)}</td><td>{label(account.content_direction)}</td><td>{account.platforms.map((identity) => <span className="identity-line" key={identity.platform}>{label(identity.platform)} · {identity.uid} · {identity.nickname || "昵称缺失"} · 实名 {label(identity.real_name_status)}</span>)}</td><td>{account.enabled ? "运营中" : "已停用"}</td><td><button className="text-button" disabled={saving} onClick={() => editAccount(account)}>修改</button></td></tr>)}
                </tbody></table></div>
              ) : <div className="empty-state"><strong>账号库尚未录入账号</strong><span>存量内容不会因缺少手机号而迁移失败；可使用“新增账号”或“批量导入”开始建库。</span></div>}
            </div>
          </section>
        )}

        {view === "contents" && (
          <section className="page-stack wide-stack">
            <div className="detail-toolbar">
              <div>
                <span className="eyebrow">{contentTotal} MIGRATED CONTENT ITEMS</span>
                <h2>内容列表</h2>
                <p>链接 ID 稳定不变；垂直度和三项业务场景值统一以百分比呈现。</p>
              </div>
              <div className="placeholder-actions">
                <a className="secondary button-link" href={`${API_BASE}/api/v8/contents/export`}>下载</a>
                <label className={`secondary button-link ${saving ? "disabled" : ""}`}>批量导入<input className="file-input" type="file" accept=".csv,text/csv" disabled={saving} onChange={(event) => { const file = event.target.files?.[0]; if (file) void importCsv(file, "content"); event.currentTarget.value = ""; }} /></label>
                <button className="primary" disabled={saving} onClick={() => editContent()}>新增内容</button>
              </div>
            </div>
            <div className="filter-bar">
              <input
                aria-label="搜索内容"
                placeholder="搜索链接 ID、标题、UID 或昵称"
                value={contentQuery}
                onChange={(event) => setContentQuery(event.target.value)}
                onKeyDown={(event) => { if (event.key === "Enter") void reloadContents(); }}
              />
              <select aria-label="平台" value={contentPlatformFilter} onChange={(event) => { setContentPlatformFilter(event.target.value); void reloadContents(reviewFilter, contentQuery, event.target.value, contentTypeFilter, contentDirectionFilter); }}><option value="">全部平台</option>{platformKeys.map((platform) => <option key={platform} value={platform}>{label(platform)}</option>)}</select>
              <select aria-label="账号类型" value={contentTypeFilter} onChange={(event) => { setContentTypeFilter(event.target.value); void reloadContents(reviewFilter, contentQuery, contentPlatformFilter, event.target.value, contentDirectionFilter); }}><option value="">全部账号类型</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option><option value="unknown">未知</option></select>
              <select aria-label="内容方向" value={contentDirectionFilter} onChange={(event) => { setContentDirectionFilter(event.target.value); void reloadContents(reviewFilter, contentQuery, contentPlatformFilter, contentTypeFilter, event.target.value); }}><option value="">全部内容方向</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option><option value="unknown">未知</option></select>
              <select
                aria-label="复核状态"
                value={reviewFilter}
                onChange={(event) => {
                  setReviewFilter(event.target.value);
                  void reloadContents(event.target.value, contentQuery, contentPlatformFilter, contentTypeFilter, contentDirectionFilter);
                }}
              >
                <option value="">全部复核状态</option>
                <option value="pending">待复核 / 补证</option>
                <option value="terminal_failed">终态不可用</option>
                <option value="resolved">无待办</option>
              </select>
              <button className="secondary" onClick={() => void reloadContents()}>搜索</button>
              <span>当前显示 {contents.length} / {contentTotal}</span>
            </div>
            <div className="panel table-panel">
              <div className="table-scroll"><table className="content-table"><thead><tr><th>内容 / 链接 ID</th><th>平台 / 日期</th><th>账号</th><th>账号类型</th><th>内容方向</th><th>卖点</th><th>垂直度</th><th>预估拉新</th><th>预估拉活</th><th>预估线索</th><th>重复提醒</th><th>阅读 / 评论</th><th>操作</th></tr></thead><tbody>
                {contents.map((item) => <tr key={item.id}>
                  <td><a href={item.canonical_url} target="_blank" rel="noreferrer">{item.title || "标题缺失"}</a><span>{item.link_id}</span></td>
                  <td>{label(item.platform)}<br />{formatDate(item.published_at)}</td>
                  <td>{item.raw_account_name || "昵称缺失"}<br />{item.raw_account_uid || "UID 缺失"}</td>
                  <td>{label(item.account_type)}</td><td>{label(item.content_direction)}</td>
                  <td>{item.primary_selling_point_code || "未命中"}<br /><span className="muted-cell">证据 {item.evidence_level || "—"}</span></td>
                  <td>{item.content_automotive_score == null ? "暂不可计算" : `${item.content_automotive_score}%`}</td>
                  <td>暂不可计算</td><td>暂不可计算</td><td>暂不可计算</td><td>{item.duplicate_original_link_id || "—"}</td>
                  <td>{item.view_count == null ? "—" : new Intl.NumberFormat("zh-CN").format(item.view_count)} / {item.comment_count == null ? "—" : new Intl.NumberFormat("zh-CN").format(item.comment_count)}<br /><span className="muted-cell">{item.metrics_captured_at ? formatDate(item.metrics_captured_at) : "尚无快照"}</span></td>
                  <td><span className="row-actions"><button className="text-button" disabled={saving} onClick={() => void updateContentData(item)}>更新数据</button><button className="text-button" disabled={saving} onClick={() => editContent(item)}>修改</button>{item.terminal_review_count > 0 ? <span className="status-badge terminal">终态</span> : item.pending_review_count > 0 ? <button className="text-button review-entry" disabled={saving} onClick={() => void openReview(item)}>待复核</button> : null}</span></td>
                </tr>)}
              </tbody></table></div>
            </div>
          </section>
        )}

        {view === "selling-points" && (
          <section className="page-stack">
            <div className="detail-toolbar">
              <div>
                <span className="eyebrow">{sellingPoints.taxonomy?.version ?? "SELLING POINT TAXONOMY"}</span>
                <h2>当前卖点基础标准</h2>
                <p>适用业务场景为集合；C1–C4 已按源文件保留多场景映射。</p>
              </div>
              <div className="placeholder-actions">
                {draftMode ? (
                  <>
                    <button className="secondary" disabled={saving} onClick={() => openPoint()}>新增卖点</button>
                    <button className="primary" disabled={saving} onClick={() => void publishTaxonomy()}>发布标准</button>
                  </>
                ) : <button className="primary" disabled={saving} onClick={() => void beginTaxonomyEdit()}>编辑卖点标准</button>}
              </div>
            </div>
            <div className="family-strip">
              {Array.from(pointFamilies.entries()).map(([family, hits]) => <div key={family}><span>{family} 系列</span><strong>{hits}</strong><small>当前 primary 命中</small></div>)}
            </div>
            <div className="selling-point-grid">
              {sellingPoints.items.map((point) => (
                <article className="panel selling-point-card" key={point.code}>
                  <div className="selling-point-code"><strong>{point.code}</strong><span>{point.tier === "core" ? "核心" : "其他"}</span></div>
                  <h3>{point.label}</h3>
                  <p>{point.definition || "定义以当前已发布词表为准。"}</p>
                  <div className="scene-tags">{point.scenes.map((scene) => <span className="scene-tag" key={scene}>{label(scene)}</span>)}</div>
                  <footer>
                    <span>{draftMode ? "草稿版本，发布前不影响评估" : `primary ${point.primary_hits ?? 0} · 全部命中 ${point.total_hits ?? 0}`}</span>
                    <span className="card-actions">
                      <button className="text-button" disabled={!draftMode || saving} onClick={() => openPoint(point)}>编辑</button>
                      {draftMode && <button className="text-button danger" disabled={saving} onClick={() => void removePoint(point.code)}>删除</button>}
                    </span>
                  </footer>
                </article>
              ))}
            </div>
          </section>
        )}

        {accountForm && (
          <div className="modal-backdrop" role="presentation">
            <section className="review-modal operation-modal" role="dialog" aria-modal="true" aria-label="编辑账号">
              <div className="panel-head"><div><span className="eyebrow">ACCOUNT RECORD</span><h3>{accountForm.id ? "修改账号" : "新增账号"}</h3><p>手机号是唯一覆盖键；页面和导出均显示完整号码。</p></div><button className="modal-close" onClick={() => setAccountForm(null)} aria-label="关闭">×</button></div>
              <div className="review-fields">
                <label>手机号<input value={accountForm.phone} onChange={(event) => setAccountForm({ ...accountForm, phone: event.target.value })} placeholder="完整手机号" /></label>
                <label>运营人员<input value={accountForm.operatorName} onChange={(event) => setAccountForm({ ...accountForm, operatorName: event.target.value })} /></label>
                <label>账号类型<select value={accountForm.accountType} onChange={(event) => setAccountForm({ ...accountForm, accountType: event.target.value })}><option value="unknown">未知</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option></select></label>
                <label>内容方向<select value={accountForm.contentDirection} onChange={(event) => setAccountForm({ ...accountForm, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label>
                <label className="toggle-field"><input type="checkbox" checked={accountForm.enabled} onChange={(event) => setAccountForm({ ...accountForm, enabled: event.target.checked })} />启用自动监控</label>
              </div>
              <div className="platform-editor">
                {platformKeys.map((platform) => {
                  const identity = accountForm.platforms[platform];
                  return <fieldset key={platform}><legend>{label(platform)}</legend><label>UID<input value={identity.uid} onChange={(event) => setAccountForm({ ...accountForm, platforms: { ...accountForm.platforms, [platform]: { ...identity, uid: event.target.value } } })} /></label><label>昵称<input value={identity.nickname} onChange={(event) => setAccountForm({ ...accountForm, platforms: { ...accountForm.platforms, [platform]: { ...identity, nickname: event.target.value } } })} /></label><label>是否实名<select value={identity.realNameStatus} onChange={(event) => setAccountForm({ ...accountForm, platforms: { ...accountForm.platforms, [platform]: { ...identity, realNameStatus: event.target.value } } })}><option value="unknown">未知</option><option value="yes">是</option><option value="no">否</option></select></label></fieldset>;
                })}
              </div>
              <div className="modal-actions"><button className="secondary" onClick={() => setAccountForm(null)}>取消</button><button className="primary" disabled={saving} onClick={() => void saveAccount()}>{saving ? "保存中" : "保存账号"}</button></div>
            </section>
          </div>
        )}

        {contentForm && (
          <div className="modal-backdrop" role="presentation">
            <section className="review-modal operation-modal" role="dialog" aria-modal="true" aria-label="编辑内容">
              <div className="panel-head"><div><span className="eyebrow">CONTENT RECORD</span><h3>{contentForm.id ? "修改内容" : "新增内容"}</h3><p>抖音、小红书必须解析出平台内容 ID；视频号、快手允许规范化链接兜底。</p></div><button className="modal-close" onClick={() => setContentForm(null)} aria-label="关闭">×</button></div>
              <div className="review-fields">
                <label>发布平台<select value={contentForm.platform} onChange={(event) => setContentForm({ ...contentForm, platform: event.target.value })}>{platformKeys.map((platform) => <option key={platform} value={platform}>{label(platform)}</option>)}</select></label>
                <label>内容类型<select value={contentForm.contentType} onChange={(event) => setContentForm({ ...contentForm, contentType: event.target.value })}><option value="video">视频</option><option value="image">图文</option><option value="unknown">未知</option></select></label>
                <label className="span-two">链接<input value={contentForm.canonicalUrl} onChange={(event) => setContentForm({ ...contentForm, canonicalUrl: event.target.value })} placeholder="https://..." /></label>
                <label>平台内容 ID<input value={contentForm.platformContentId} onChange={(event) => setContentForm({ ...contentForm, platformContentId: event.target.value })} /></label>
                <label>发布日期<input type="datetime-local" value={contentForm.publishedAt} onChange={(event) => setContentForm({ ...contentForm, publishedAt: event.target.value })} /></label>
                <label className="span-two">标题<input value={contentForm.title} onChange={(event) => setContentForm({ ...contentForm, title: event.target.value })} /></label>
                <label className="span-two">正文<textarea value={contentForm.body} onChange={(event) => setContentForm({ ...contentForm, body: event.target.value })} /></label>
                <label>账号 UID<input value={contentForm.accountUid} onChange={(event) => setContentForm({ ...contentForm, accountUid: event.target.value })} /></label>
                <label>账号昵称<input value={contentForm.accountName} onChange={(event) => setContentForm({ ...contentForm, accountName: event.target.value })} /></label>
                <label>账号类型<select value={contentForm.accountType} onChange={(event) => setContentForm({ ...contentForm, accountType: event.target.value })}><option value="unknown">未知</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option></select></label>
                <label>内容方向<select value={contentForm.contentDirection} onChange={(event) => setContentForm({ ...contentForm, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label>
              </div>
              <div className="modal-actions"><button className="secondary" onClick={() => setContentForm(null)}>取消</button><button className="primary" disabled={saving} onClick={() => void saveContent()}>{saving ? "保存中" : "保存内容"}</button></div>
            </section>
          </div>
        )}

        {reviewItem && (
          <div className="modal-backdrop" role="presentation">
            <section className="review-modal" role="dialog" aria-modal="true" aria-label="人工复核">
              <div className="panel-head">
                <div><span className="eyebrow">MANUAL REVIEW</span><h3>{reviewItem.link_id} · {reviewItem.title || "标题缺失"}</h3></div>
                <button className="modal-close" onClick={() => setReviewItem(null)} aria-label="关闭">×</button>
              </div>
              <div className="review-fields">
                <label>复核结论<select value={reviewForm.decision} onChange={(event) => setReviewForm({ ...reviewForm, decision: event.target.value as ReviewForm["decision"] })}><option value="insufficient_evidence">证据不足</option><option value="confirm">确认自动结论</option><option value="override">人工改判</option><option value="terminal_unavailable">内容终态不可用</option></select></label>
                <label>复核人员<input value={reviewForm.reviewer} onChange={(event) => setReviewForm({ ...reviewForm, reviewer: event.target.value })} /></label>
                <label>证据类型<select value={reviewForm.evidenceType} onChange={(event) => setReviewForm({ ...reviewForm, evidenceType: event.target.value })}><option value="review_note">复核说明</option><option value="visual_summary">画面摘要</option><option value="media_observation">媒体观察</option></select></label>
                <label>复核原因<input value={reviewForm.reason} onChange={(event) => setReviewForm({ ...reviewForm, reason: event.target.value })} placeholder="说明判定理由" /></label>
              </div>
              <label className="review-reason">人工证据<textarea value={reviewForm.evidenceText} onChange={(event) => setReviewForm({ ...reviewForm, evidenceText: event.target.value })} placeholder="记录可复查的文本、画面或可用性证据" /></label>
              {reviewForm.decision === "override" && <div className="review-fields override-fields">
                <label>卖点编码<input value={reviewForm.primaryCode} onChange={(event) => setReviewForm({ ...reviewForm, primaryCode: event.target.value.toUpperCase() })} placeholder="例如 C1" /></label>
                <label>卖点分<input type="number" min="0" max="100" value={reviewForm.sellingScore} onChange={(event) => setReviewForm({ ...reviewForm, sellingScore: event.target.value })} /></label>
                <label>垂直度<input type="number" min="0" max="100" value={reviewForm.automotiveScore} onChange={(event) => setReviewForm({ ...reviewForm, automotiveScore: event.target.value })} /></label>
                <label>内容方向<select value={reviewForm.contentDirection} onChange={(event) => setReviewForm({ ...reviewForm, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label>
              </div>}
              <div className="modal-actions"><button className="secondary" onClick={() => setReviewItem(null)}>取消</button><button className="primary" disabled={saving} onClick={() => void submitReview()}>{saving ? "提交中" : "提交复核"}</button></div>
            </section>
          </div>
        )}

        {pointForm && (
          <div className="modal-backdrop" role="presentation">
            <section className="review-modal" role="dialog" aria-modal="true" aria-label="编辑卖点">
              <div className="panel-head"><div><span className="eyebrow">SELLING POINT DRAFT</span><h3>{editingPointCode ? `编辑 ${editingPointCode}` : "新增卖点"}</h3></div><button className="modal-close" onClick={() => setPointForm(null)} aria-label="关闭">×</button></div>
              <div className="review-fields">
                <label>编码<input value={pointForm.code} disabled={Boolean(editingPointCode)} onChange={(event) => setPointForm({ ...pointForm, code: event.target.value.toUpperCase() })} placeholder="例如 M7" /></label>
                <label>层级<select value={pointForm.tier} onChange={(event) => setPointForm({ ...pointForm, tier: event.target.value as PointForm["tier"] })}><option value="core">核心</option><option value="other">其他</option></select></label>
                <label className="span-two">名称<input value={pointForm.label} onChange={(event) => setPointForm({ ...pointForm, label: event.target.value })} /></label>
                <label className="span-two">定义<textarea value={pointForm.definition} onChange={(event) => setPointForm({ ...pointForm, definition: event.target.value })} /></label>
              </div>
              <fieldset className="scene-picker"><legend>适用业务场景（至少一项）</legend>{["new_car", "used_car", "media"].map((scene) => <label key={scene}><input type="checkbox" checked={pointForm.scenes.includes(scene)} onChange={(event) => setPointForm({ ...pointForm, scenes: event.target.checked ? [...pointForm.scenes, scene] : pointForm.scenes.filter((value) => value !== scene) })} />{label(scene)}</label>)}</fieldset>
              <div className="review-fields evidence-fields">
                <label>正向证据（每行一条）<textarea value={pointForm.positiveEvidence} onChange={(event) => setPointForm({ ...pointForm, positiveEvidence: event.target.value })} /></label>
                <label>负向证据（每行一条）<textarea value={pointForm.negativeEvidence} onChange={(event) => setPointForm({ ...pointForm, negativeEvidence: event.target.value })} /></label>
                <label className="span-two">边界规则（每行一条）<textarea value={pointForm.boundaryRules} onChange={(event) => setPointForm({ ...pointForm, boundaryRules: event.target.value })} /></label>
              </div>
              <div className="modal-actions"><button className="secondary" onClick={() => setPointForm(null)}>取消</button><button className="primary" disabled={saving} onClick={() => void savePoint()}>{saving ? "保存中" : "保存草稿"}</button></div>
            </section>
          </div>
        )}

        {taskModal && (
          <div className="modal-backdrop" role="presentation">
            <section className="review-modal compact-modal" role="dialog" aria-modal="true" aria-label="新建报告任务">
              <div className="panel-head"><div><span className="eyebrow">CUSTOM REPORT</span><h3>新建自定义报告</h3><p>按发布日期选择闭区间；生成报告只读已落库数据，不隐式触发付费抓取。</p></div><button className="modal-close" onClick={() => setTaskModal(false)} aria-label="关闭">×</button></div>
              <div className="review-fields">
                <label>开始日期<input type="date" value={taskForm.periodStart} onChange={(event) => setTaskForm({ ...taskForm, periodStart: event.target.value })} /></label>
                <label>结束日期<input type="date" value={taskForm.periodEnd} onChange={(event) => setTaskForm({ ...taskForm, periodEnd: event.target.value })} /></label>
                <label className="span-two">任务名称（可不填）<input value={taskForm.name} onChange={(event) => setTaskForm({ ...taskForm, name: event.target.value })} placeholder="自动使用日期区间命名" /></label>
              </div>
              <div className="modal-actions"><button className="secondary" onClick={() => setTaskModal(false)}>取消</button><button className="primary" disabled={saving} onClick={() => void createReportTask()}>{saving ? "生成中" : "生成报告"}</button></div>
            </section>
          </div>
        )}

        {taskDetail && (
          <div className="modal-backdrop task-detail-backdrop" role="presentation">
            <section className="review-modal task-detail-modal" role="dialog" aria-modal="true" aria-label="任务详情">
              <div className="panel-head"><div><span className="eyebrow">{taskDetail.id}</span><h3>{taskDetail.name}</h3><p>{formatDate(taskDetail.period_start)} — {formatDate(taskDetail.period_end)} · {label(taskDetail.task_status)}</p></div><button className="modal-close" onClick={() => setTaskDetail(null)} aria-label="关闭">×</button></div>
              <div className="task-tabs" role="tablist">{[["summary", "报告概览"], ["platforms", "平台维度"], ["dimensions", "账号 / 方向"], ["contents", "内容明细"], ["files", "文件与日志"]].map(([id, text]) => <button key={id} className={taskTab === id ? "active" : ""} onClick={() => setTaskTab(id)}>{text}</button>)}</div>
              {taskTab === "summary" && <div className="task-tab-body"><div className="quality-grid"><div><strong>{taskReport ? metricValue(taskReport.summary_metrics.publication_count) : "—"}</strong><span>发布内容</span></div><div><strong>{taskReport ? metricValue(taskReport.summary_metrics.verticality_rate) : "—"}</strong><span>内容垂直度</span></div><div><strong>{taskReport ? metricValue(taskReport.summary_metrics.selling_point_coverage_rate) : "—"}</strong><span>卖点覆盖率</span></div></div><dl className="definition-list">{Object.entries(taskReport?.data_quality ?? {}).map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{value}%</dd></div>)}</dl>{["partial", "failed", "interrupted"].includes(taskDetail.task_status) && <button className="secondary promote-button" disabled={saving} onClick={() => void retryTask()}>基于当前证据生成新 revision</button>}</div>}
              {taskTab === "platforms" && <div className="task-tab-body dimension-list">{taskReport?.platform_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div>}
              {taskTab === "dimensions" && <div className="task-tab-body"><h4>账号类型</h4><div className="dimension-list">{taskReport?.account_type_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div><h4>内容方向</h4><div className="dimension-list">{taskReport?.content_direction_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div></div>}
              {taskTab === "contents" && <div className="task-tab-body table-scroll"><table><thead><tr><th>链接 ID</th><th>平台</th><th>标题</th><th>证据</th><th>垂直度</th></tr></thead><tbody>{taskReport?.content_details.map((item) => <tr key={String(item.content_id)}><td>{item.link_id}</td><td>{label(String(item.platform))}</td><td>{item.title || "标题缺失"}</td><td>{item.evidence_level || "—"}</td><td>{item.content_automotive_score == null ? "暂不可计算" : `${item.content_automotive_score}%`}</td></tr>)}</tbody></table></div>}
              {taskTab === "files" && <div className="task-tab-body"><div className="download-list">{taskDetail.revisions.map((revision) => <article key={revision.revision}><strong>Revision {revision.revision}</strong><span>{formatDate(revision.created_at)}</span><div>{revision.files.map((file) => <a key={file.file_kind} href={`${API_BASE}/api/v8/tasks/${taskDetail.id}/revisions/${revision.revision}/files/${file.file_kind}`} target="_blank" rel="noreferrer">{file.file_kind} · {new Intl.NumberFormat("zh-CN").format(file.byte_size)} B</a>)}</div></article>)}</div><h4>任务日志</h4><ol className="event-list">{taskDetail.events.map((event) => <li key={event.id}><strong>{event.event_type}</strong><span>{event.message} · {formatDate(event.created_at)}</span></li>)}</ol></div>}
            </section>
          </div>
        )}
      </main>
    </div>
  );
}
