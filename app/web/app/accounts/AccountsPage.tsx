"use client";

import Image from "next/image";
import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import { CircleIcon, CopyIcon, DotsThreeVerticalIcon, VideoCameraIcon } from "@phosphor-icons/react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppShell from "../components/AppShell";
import DataFreshnessNote from "../components/DataFreshnessNote";
import AccountPageAccess from "../components/AccountPageAccess";
import { Feedback, Loading, Notice, ReadErrorState } from "../components/Feedback";
import { AccountsPagination } from "./AccountsPagination";
import CreateAccountDialog, { type CreateAccountResponse } from "./CreateAccountDialog";
import RepairAccountIdentityDialog from "./RepairAccountIdentityDialog";
import AccountSummaryDialog from "./AccountSummaryDialog";
import AccountOperationsDialog, { type AccountForm } from "./AccountOperationsDialog";
import AccountDialogLayout from "./AccountDialogLayout";
import { apiErrorMessage, apiUrl, handleApprovalRequired, jsonRequest, readJson } from "../lib/api";
import { PLATFORM_LOGO_PATHS } from "../lib/contentMedia";
import { formatDateTime, label, platformKeys } from "../lib/format";
import { accountGroupOptions, businessDirectionOptions, accountGroupLabel, businessDirectionLabel } from "../lib/accountClassification";
import { publicAssetPath } from "../lib/paths";
import styles from "./accounts.module.css";
import { buildAccountSearchRequest, lastPageFor, type AccountSearchRequest } from "../lib/queryContracts";
import { accountDirectoryDetailQueryOptions, accountSearchQueryOptions, defaultAccountSearchRequest, douyinAuthorizationStatusesQueryOptions, queryKeys, type AccountSearchResult } from "../lib/queries";
import type { Account, AccountStatus } from "../lib/types";
const statusLabels: Record<string, string> = {
  monitored: "已监测", not_monitored: "未监测", authorized: "已授权",
  unauthorized: "未授权", unknown: "未知", not_collected: "未采集",
  stale: "待更新", available: "已更新",
};
const accountStatusLabels: Record<AccountStatus, string> = {
  daily: "日更", weekly: "周更", paused: "暂停", unmarked: "待标记",
};
const accountStatusActions = [
  { status: "daily", label: "改为日更" },
  { status: "weekly", label: "改为周更" },
  { status: "paused", label: "暂停账号" },
] as const;
const accountStatusHints: Record<AccountStatus, string> = {
  daily: "仅标注每天更新作品的运营频率。",
  weekly: "仅标注每周更新作品的运营频率。",
  paused: "人工标记为暂停；当前仍按采集条件参与自动采集。",
  unmarked: "尚未标记作品更新频率。",
};
const integerFormat = new Intl.NumberFormat("zh-CN");
const platformAccountLabels: Record<string, string> = {
  douyin: "抖音号", xiaohongshu: "小红书号", kuaishou: "快手号", wechat_channels: "视频号",
};
function positionAccountMenu(menu: HTMLDetailsElement) {
  if (!menu.open) return;
  document.querySelectorAll<HTMLDetailsElement>("details[data-account-menu][open]").forEach((other) => {
    if (other !== menu) other.open = false;
  });
  const anchor = menu.querySelector("summary");
  const panel = menu.querySelector<HTMLElement>("[data-account-menu-panel]");
  if (!anchor || !panel) return;
  const trigger = anchor.getBoundingClientRect();
  const size = panel.getBoundingClientRect();
  const below = trigger.bottom + 4;
  panel.style.left = `${Math.max(8, Math.min(trigger.right - size.width, window.innerWidth - size.width - 8))}px`;
  panel.style.top = `${below + size.height <= window.innerHeight - 8 ? below : Math.max(8, trigger.top - size.height - 4)}px`;
  panel.style.visibility = "visible";
}

function formatIdentityCount(value: number | null | undefined, suffix = "") {
  return value == null ? "—" : `${integerFormat.format(value)}${suffix}`;
}

function workbookFilename(disposition: string | null) {
  const encoded = disposition?.match(/filename\*=UTF-8''([^;]+)/i)?.[1];
  if (encoded) {
    try { return decodeURIComponent(encoded); } catch { /* Fall through to the safe filename. */ }
  }
  return "账号表格.xlsx";
}

function PlatformHeaderMark({ platformKey }: { platformKey: string }) {
  const logoPath = PLATFORM_LOGO_PATHS[platformKey];
  return <span className={styles.platformMark} data-platform-mark={platformKey} data-official-logo={logoPath ? "true" : undefined} aria-hidden="true">
    {logoPath ? <Image src={publicAssetPath(logoPath)} alt="" width={32} height={32} unoptimized /> : <VideoCameraIcon weight="fill" />}
  </span>;
}

export default function AccountsPage() {
  return <AccountPageAccess><AccountsWorkspace /></AccountPageAccess>;
}

function AccountsWorkspace() {
  const [query, setQuery] = useState("");
  const [accountGroup, setAccountGroup] = useState("");
  const [accountStatus, setAccountStatus] = useState<AccountStatus | "">("");
  const [businessDirection, setBusinessDirection] = useState("");
  const [platform, setPlatform] = useState("");
  const [upload, setUpload] = useState<File | null>(null);
  const [exportEvidence, setExportEvidence] = useState({ organization: "", exportedAt: "", recordId: "", declaredCount: "", evidence: "" });
  const [appliedRequest, setAppliedRequest] = useState(() => ({ ...defaultAccountSearchRequest }));
  const [form, setForm] = useState<AccountForm | null>(null);
  const [editingAccount, setEditingAccount] = useState<Account | null>(null);
  const [summaryAccount, setSummaryAccount] = useState<Account | null>(null);
  const [detailRead, setDetailRead] = useState<{ directoryRowId: number; error: string } | null>(null);
  const detailIntent = useRef(0);
  const summaryTrigger = useRef<HTMLButtonElement | null>(null);
  const [creatingAccount, setCreatingAccount] = useState(false);
  const [identityAccount, setIdentityAccount] = useState<Account | null>(null);
  const [saving, setSaving] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [lastIntakeId, setLastIntakeId] = useState<number | null>(null);
  const statusRequests = useRef(new Map<number, { intent: string; requestId: string }>());
  const queryClient = useQueryClient();
  const prefetchGeneration = useRef(0);
  const prefetchIntentTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const prefetchWork = useRef<{
    running: { request: AccountSearchRequest; generation: number } | null;
    queued: { request: AccountSearchRequest; generation: number } | null;
  }>({ running: null, queued: null });
  const accountsQuery = useQuery(accountSearchQueryOptions(appliedRequest));
  const [lastResult, setLastResult] = useState<AccountSearchResult | null>(null);
  // Keep the last completed page available after a failed page transition.
  // AccountPageAccess remounts this workspace whenever the permission scope changes.
  useEffect(() => {
    if (accountsQuery.data && !accountsQuery.isPlaceholderData && !accountsQuery.isError) {
      setLastResult(accountsQuery.data);
    }
  }, [accountsQuery.data, accountsQuery.isPlaceholderData, accountsQuery.isError]);
  const result = accountsQuery.data ?? lastResult ?? undefined;
  const items = result?.items ?? [];
  const total = result?.total ?? 0;
  const accountManagementVersion = result?.account_management_version ?? 1;
  const exportUnavailable = accountManagementVersion < 2 && Boolean(appliedRequest.account_status);
  const exportUnavailableMessage = "服务更新完成后可按账号状态导出。";
  const managedMode = result?.roster?.source_family === "system" && result.roster.active_profile_id != null;
  const accountsReadFailed = !result && (accountsQuery.isLoadingError || retrying);
  const displayingPrevious = Boolean(result && (result.sourceRequest
    ? JSON.stringify(result.sourceRequest) !== JSON.stringify(appliedRequest)
    : accountsQuery.isPlaceholderData));
  const sourcePage = result?.sourceRequest?.page ?? appliedRequest.page;
  const readStatus = accountsQuery.isError
    ? `第 ${appliedRequest.page} 页读取失败${result ? `，当前显示上次读取的第 ${sourcePage} 页` : ""}`
    : displayingPrevious
      ? `正在读取第 ${appliedRequest.page} 页，暂显示上次条件的第 ${sourcePage} 页`
      : accountsQuery.isFetching && result
        ? `第 ${appliedRequest.page} 页已显示，正在更新`
        : `共 ${total} 个账号`;

  const cancelPagePrefetchIntent = useCallback(() => {
    if (prefetchIntentTimer.current !== null) clearTimeout(prefetchIntentTimer.current);
    prefetchIntentTimer.current = null;
    prefetchWork.current.queued = null;
  }, []);

  const startPagePrefetch = useCallback(function prefetch(request: AccountSearchRequest, generation: number, explicit = false) {
    if (generation !== prefetchGeneration.current) return;
    const work = prefetchWork.current;
    if (work.running) {
      // A single speculative request may run. Retain only the last explicit
      // pointer/keyboard target; automatic next-page warming never jumps the queue.
      if (JSON.stringify(work.running.request) === JSON.stringify(request)) {
        if (explicit) work.queued = null;
      } else if (explicit) work.queued = { request, generation };
      return;
    }
    const running = { request, generation };
    work.running = running;
    const finish = () => {
      if (work.running !== running) return;
      work.running = null;
      const queued = work.queued;
      work.queued = null;
      if (queued) prefetch(queued.request, queued.generation, true);
    };
    void queryClient.prefetchQuery(accountSearchQueryOptions(request)).then(finish, finish);
  }, [queryClient]);

  function prefetchPage(page: number) {
    cancelPagePrefetchIntent();
    if (!result || result.list_contract_version !== 1 || displayingPrevious
      || accountsQuery.isFetching || accountsQuery.isError || page === appliedRequest.page
      || !Number.isSafeInteger(page) || page < 1 || page > lastPageFor(total, appliedRequest.page_size)) return;
    const request = { ...appliedRequest, page };
    const generation = prefetchGeneration.current;
    prefetchIntentTimer.current = setTimeout(() => {
      prefetchIntentTimer.current = null;
      startPagePrefetch(request, generation, true);
    }, 120);
  }

  useEffect(() => {
    const work = prefetchWork.current;
    const generation = ++prefetchGeneration.current;
    let timer: ReturnType<typeof setTimeout> | undefined;
    if (!accountsQuery.data || accountsQuery.isPlaceholderData || accountsQuery.isFetching
      || accountsQuery.isError || accountsQuery.data.list_contract_version !== 1
      || appliedRequest.page >= lastPageFor(accountsQuery.data.total, appliedRequest.page_size)) {
      // Still install cleanup: this generation may receive explicit intent later.
    } else {
      const nextRequest = { ...appliedRequest, page: appliedRequest.page + 1 };
      timer = setTimeout(() => startPagePrefetch(nextRequest, generation), 120);
    }
    return () => {
      prefetchGeneration.current += 1;
      if (timer !== undefined) clearTimeout(timer);
      cancelPagePrefetchIntent();
      // A click may already be observing this page. Cancel only unused speculation.
      // Passive cleanup can run before the destination observer subscribes.
      // Wait for the current commit before deciding whether this read is unused.
      queueMicrotask(() => {
        const running = work.running;
        if (!running || running.generation === prefetchGeneration.current) return;
        const options = accountSearchQueryOptions(running.request);
        const cached = queryClient.getQueryCache().find({ queryKey: options.queryKey, exact: true });
        if (cached && cached.getObserversCount() === 0) void queryClient.cancelQueries({ queryKey: options.queryKey, exact: true });
      });
    };
  }, [appliedRequest, accountsQuery.data, accountsQuery.isPlaceholderData, accountsQuery.isFetching, accountsQuery.isError, queryClient, cancelPagePrefetchIntent, startPagePrefetch]);

  useEffect(() => () => { detailIntent.current += 1; }, []);

  useEffect(() => {
    const repositionMenus = () => document.querySelectorAll<HTMLDetailsElement>("details[data-account-menu][open]").forEach(positionAccountMenu);
    window.addEventListener("scroll", repositionMenus, true);
    window.addEventListener("resize", repositionMenus);
    return () => {
      window.removeEventListener("scroll", repositionMenus, true);
      window.removeEventListener("resize", repositionMenus);
    };
  }, []);

  function retryAccountsRead() {
    if (retrying) return;
    setRetrying(true);
    void accountsQuery.refetch().finally(() => setRetrying(false));
  }

  function applySearch(overrides: Partial<{ query: string; accountGroup: string; accountStatus: AccountStatus | ""; businessDirection: string; platform: string; page: number; pageSize: number }> = {}) {
    const filters = { query, accountGroup, accountStatus, businessDirection, platform, ...overrides };
    const nextRequest = buildAccountSearchRequest(
      filters,
      overrides.page ?? appliedRequest.page,
      overrides.pageSize ?? appliedRequest.page_size,
    );
    if (JSON.stringify(nextRequest) === JSON.stringify(appliedRequest)) {
      if (accountsReadFailed) retryAccountsRead();
      else void accountsQuery.refetch();
      return;
    }
    setAppliedRequest(nextRequest);
  }

  function changePage(next: { page: number; pageSize?: number }) {
    // Pagination uses applied filters; typing a draft search must not silently apply it.
    setAppliedRequest((current) => ({ ...current, page: next.page, page_size: next.pageSize ?? current.page_size }));
  }

  useEffect(() => {
    if (!result || displayingPrevious || accountsQuery.isError) return;
    const lastPage = lastPageFor(result.total, appliedRequest.page_size);
    if (appliedRequest.page > lastPage) {
      const timer = window.setTimeout(() => {
        setAppliedRequest((current) => JSON.stringify(current) === JSON.stringify(appliedRequest)
          ? { ...current, page: lastPage } : current);
      }, 0);
      return () => window.clearTimeout(timer);
    }
  }, [displayingPrevious, accountsQuery.isError, appliedRequest, result]);

  async function invalidateAccountData() {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.accounts }),
      queryClient.invalidateQueries({ queryKey: queryKeys.contents }),
      queryClient.invalidateQueries({ queryKey: queryKeys.overview }),
      queryClient.invalidateQueries({ queryKey: queryKeys.sellingPoints }),
      queryClient.invalidateQueries({ queryKey: queryKeys.spu }),
    ]);
  }

  function edit(account: Account) {
    setError(""); setMessage("");
    setEditingAccount(account);
    const status = account.account_status && account.account_status !== "unmarked" ? account.account_status : "";
    setForm({ id: account.id, phone: account.phone, operatorName: account.operator_name, accountGroup: account.account_group, businessDirection: account.business_direction, accountStatus: status, originalAccountStatus: status });
  }

  async function openAccountAction(account: Account, action: "summary" | "edit" | "identity") {
    if (saving || displayingPrevious) return;
    const intent = ++detailIntent.current;
    setError("");
    let fullAccount = account;
    if (result?.list_contract_version === 1) {
      const directoryRowId = account.directory_row_id;
      if (!Number.isSafeInteger(directoryRowId) || !directoryRowId || directoryRowId <= 0) { setError("账号资料定位信息缺失，请刷新列表。"); return; }
      setDetailRead({ directoryRowId, error: "" });
      try {
        fullAccount = await queryClient.fetchQuery(accountDirectoryDetailQueryOptions(directoryRowId));
      } catch (reason) {
        if (intent === detailIntent.current) setDetailRead({ directoryRowId, error: reason instanceof Error ? reason.message : "账号资料读取失败，请关闭后重试。" });
        return;
      }
      if (intent !== detailIntent.current) return;
      setDetailRead(null);
    }
    if (action === "summary") setSummaryAccount(fullAccount);
    else if (action === "edit") edit(fullAccount);
    else setIdentityAccount(fullAccount);
  }

  function closeDetailRead() {
    detailIntent.current += 1;
    if (detailRead) void queryClient.cancelQueries({ queryKey: queryKeys.accountDirectoryDetail(detailRead.directoryRowId), exact: true });
    setDetailRead(null);
    summaryTrigger.current?.focus();
  }

  function closeAccountSummary() {
    setSummaryAccount(null);
    summaryTrigger.current?.focus();
  }

  async function copyAccountIdentifier(value: string, fieldLabel: string) {
    setError(""); setMessage("");
    try {
      await navigator.clipboard.writeText(value);
      setMessage(`${fieldLabel}已复制`);
    } catch {
      setError(`复制失败，请在编号提示中查看完整${fieldLabel}后手动复制。`);
    }
  }

  function statusRequestId(accountId: number, body: object) {
    const intent = JSON.stringify(body);
    const pending = statusRequests.current.get(accountId);
    if (pending?.intent === intent) return pending.requestId;
    const requestId = crypto.randomUUID();
    // Keep only the current intent: returning to an earlier status is a new operation.
    statusRequests.current.set(accountId, { intent, requestId });
    return requestId;
  }

  async function save() {
    if (!form || saving) return;
    setSaving(true); setError(""); setMessage("");
    try {
      const body = {
        ...(form.id > 0 ? { phone: form.phone, operator_name: form.operatorName } : {}), account_group: form.accountGroup, business_direction: form.businessDirection,
        ...(form.id > 0 && form.accountStatus && form.accountStatus !== form.originalAccountStatus ? { account_status: form.accountStatus } : {}),
      };
      const requestId = body.account_status ? statusRequestId(form.id, body) : undefined;
      const response = await readJson<{ message: string }>(`/api/v8/accounts/${form.id}`, jsonRequest({
        ...body,
        ...(body.account_status ? { status_request_id: requestId } : {}),
      }, "PATCH"));
      if (body.account_status) statusRequests.current.delete(form.id);
      setForm(null); await invalidateAccountData(); setMessage(response.message);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号保存失败"); }
    finally { setSaving(false); }
  }

  function accountCreated(response: CreateAccountResponse) {
    setCreatingAccount(false);
    setLastIntakeId(response.intake_id ?? null);
    setQuery(response.uid);
    setAccountGroup("");
    setAccountStatus("");
    setBusinessDirection("");
    setPlatform("");
    applySearch({ query: response.uid, accountGroup: "", accountStatus: "", businessDirection: "", platform: "", page: 1 });
    setMessage(response.message);
    void invalidateAccountData().catch(() => setError("账号已添加，但列表刷新失败，请重新搜索。"));
  }

  async function refreshIntake() {
    if (!lastIntakeId || saving) return;
    setSaving(true);
    try {
      const response = await readJson<CreateAccountResponse>(`/api/v8/accounts/intake/${lastIntakeId}`);
      setMessage(response.message);
      if (response.uid) {
        setQuery(response.uid);
        applySearch({ query: response.uid, page: 1 });
      }
      await invalidateAccountData();
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号资料补齐结果读取失败"); }
    finally { setSaving(false); }
  }

  async function changeAccountStatus(account: Account, status: Exclude<AccountStatus, "unmarked">) {
    if (saving || account.account_status === status) return;
    setSaving(true); setError(""); setMessage("");
    try {
      const body = { account_status: status };
      const requestId = statusRequestId(account.id, body);
      const response = await readJson<{ message: string }>(`/api/v8/accounts/${account.id}`, jsonRequest({
        ...body, status_request_id: requestId,
      }, "PATCH"));
      statusRequests.current.delete(account.id);
      await invalidateAccountData(); setMessage(response.message);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号状态修改失败"); }
    finally { setSaving(false); }
  }

  async function importRoster() {
    if (!upload) return;
    setSaving(true); setError(""); setMessage("");
    try {
      if (upload.size > 20 * 1024 * 1024) throw new Error("完整导出文件不能超过20MB。");
      const bytes = new Uint8Array(await upload.arrayBuffer());
      let binary = "";
      for (let offset = 0; offset < bytes.length; offset += 8192) binary += String.fromCharCode(...bytes.subarray(offset, offset + 8192));
      const response = await readJson<{ message: string }>("/api/v8/account-roster/import", jsonRequest({
        source_name: upload.name, content_base64: btoa(binary),
        organization: exportEvidence.organization, source_exported_at: exportEvidence.exportedAt,
        source_instance_id: exportEvidence.recordId, declared_count: Number(exportEvidence.declaredCount),
        evidence_note: exportEvidence.evidence,
      }));
      setUpload(null); await invalidateAccountData(); setMessage(response.message);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "官方名册导入失败"); }
    finally { setSaving(false); }
  }

  async function exportWorkbook() {
    if (exportUnavailable) { setError(exportUnavailableMessage); return; }
    setExporting(true); setError(""); setMessage("");
    try {
      let authorizationTargets: Array<{ account_id: number; platform_uid: string; state: "authorized" | "needs_reauthorization" }> | null = null;
      try {
        const statuses = await queryClient.fetchQuery(douyinAuthorizationStatusesQueryOptions());
        authorizationTargets = statuses.items
          .filter((item) => item.status === "active" && item.account_id != null && item.platform_uid != null)
          .map((item) => ({
            account_id: item.account_id as number,
            platform_uid: item.platform_uid as string,
            state: item.authorized ? "authorized" as const : "needs_reauthorization" as const,
          }));
      } catch {
        authorizationTargets = null;
      }
      let response: Response;
      try {
        response = await fetch(apiUrl("/api/v8/accounts/export"), jsonRequest({ douyin_authorization_targets: authorizationTargets, query: appliedRequest.query, platform: appliedRequest.platform, account_group: appliedRequest.account_group, business_direction: appliedRequest.business_direction, ...(accountManagementVersion >= 2 ? { account_status: appliedRequest.account_status } : { scope: "all" }) }));
      } catch {
        throw new Error("无法连接数据服务，请检查网络或稍后重试。");
      }
      if (!response.ok) {
        const body = (await response.json().catch(() => null)) as { detail?: unknown; code?: string } | null;
        if (handleApprovalRequired(response.status, body?.code)) return;
        throw new Error(apiErrorMessage(body?.detail, response.status, body?.code));
      }
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = workbookFilename(response.headers.get("Content-Disposition"));
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "账号表格导出失败，请稍后重试。");
    } finally {
      setExporting(false);
    }
  }

  return <AppShell active="accounts" header={accountsQuery.isPending && !result && !accountsReadFailed ? undefined :
    <header className="page-header"><div className="page-header-copy"><span className="page-header-eyebrow">{managedMode ? "系统账号" : "矩阵通名册"}</span><h1 className="page-header-title">账号信息</h1><p className="page-header-description">未采集的粉丝和平台作品总量显示“—”。</p></div><div className="page-header-actions">{managedMode ? <button className="primary" disabled={saving} onClick={() => { setError(""); setMessage(""); setCreatingAccount(true); }}>新增系统账号</button> : <label className="secondary button-link">批量上传账号<input className="file-input" type="file" accept=".xlsx,.csv,.json" disabled={saving} onChange={(event) => { const file = event.target.files?.[0]; if (file) setUpload(file); event.currentTarget.value = ""; }} /></label>}<button className="secondary button-link" disabled={exporting || exportUnavailable} title={exportUnavailable ? exportUnavailableMessage : undefined} onClick={() => void exportWorkbook()}>{exporting ? "正在导出…" : "下载账号表格"}</button></div></header>
  }>
    <Feedback error={form ? "" : error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {lastIntakeId !== null && <p><button className="secondary" disabled={saving} onClick={() => void refreshIntake()}>刷新最近账号的资料补齐结果</button></p>}
    {accountsQuery.isError && <Notice tone="error">{result ? `数据刷新失败，当前显示上次数据。${accountsQuery.error instanceof Error ? accountsQuery.error.message : ""}` : accountsQuery.error instanceof Error ? accountsQuery.error.message : "账号读取失败"}
      {result && <><button type="button" className="secondary" disabled={accountsQuery.isFetching} onClick={retryAccountsRead}>重试第 {appliedRequest.page} 页</button>{displayingPrevious && result.sourceRequest && <button type="button" className="secondary" onClick={() => setAppliedRequest(result.sourceRequest!)}>继续查看第 {sourcePage} 页</button>}</>}
    </Notice>}
    {accountsQuery.isPending && !result && !accountsReadFailed ? <Loading label="正在读取账号库" /> : <section className="page-stack wide-stack">
      <div className="filter-bar"><select aria-label="账号状态筛选" value={accountStatus} onChange={(event) => { const nextStatus = event.target.value as AccountStatus | ""; setAccountStatus(nextStatus); applySearch({ accountStatus: nextStatus, page: 1 }); }}><option value="">全部账号状态</option><option value="daily">日更</option><option value="weekly">周更</option><option value="paused">暂停</option><option value="unmarked">待标记</option></select><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="手机号、运营人员、平台账号编号、昵称" onKeyDown={(event) => { if (event.key === "Enter") applySearch({ page: 1 }); }} /><select aria-label="账号分组筛选" value={accountGroup} onChange={(event) => { setAccountGroup(event.target.value); applySearch({ accountGroup: event.target.value, page: 1 }); }}><option value="">全部账号分组</option>{accountGroupOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select><select aria-label="业务方向筛选" value={businessDirection} onChange={(event) => { setBusinessDirection(event.target.value); applySearch({ businessDirection: event.target.value, page: 1 }); }}><option value="">全部业务方向</option>{businessDirectionOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select><select value={platform} onChange={(event) => { setPlatform(event.target.value); applySearch({ platform: event.target.value, page: 1 }); }}><option value="">全部平台</option>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select><button className="secondary" onClick={() => applySearch({ page: 1 })}>搜索</button><span>{accountsReadFailed ? "读取失败" : `${total} 个账号`}</span></div>
      {exportUnavailable && <p role="status">{exportUnavailableMessage}</p>}
      <article aria-busy={accountsQuery.isFetching}>
        {!accountsReadFailed && <div className={styles.tableTitle}><h2>账号列表</h2><span>共 {total} 个账号</span></div>}
        {result && !accountsReadFailed && <DataFreshnessNote />}
        {(accountsQuery.isError || displayingPrevious || (accountsQuery.isFetching && result)) && <p data-account-page-status>{readStatus}</p>}
        <div className={`${styles.accountPanel}${accountsReadFailed ? " has-read-error" : ""}`}>
        <div className={`table-scroll ${styles.tableScroll}`}><table className={styles.memberTable}>
        <caption className="visually-hidden">{managedMode ? "系统账号" : "矩阵通账号名单"}：一个平台账号一行</caption>
        <colgroup><col className={styles.accountColumn} /><col className={styles.rosterColumn} /><col className={styles.phoneColumn} /><col className={styles.metricColumn} /><col className={styles.metricColumn} /><col className={styles.metricColumn} /><col className={styles.operatorColumn} /><col className={styles.classificationColumn} /><col className={styles.actionsColumn} /></colgroup>
        <thead>
          <tr><th scope="col" rowSpan={2} className={styles.accountHeading}>账号</th><th scope="col" rowSpan={2}>账号状态</th><th scope="col" rowSpan={2}>手机号</th><th scope="colgroup" colSpan={3}>数据规模</th><th scope="col" rowSpan={2}>运营人员</th><th scope="col" rowSpan={2}>账号分类</th><th scope="col" rowSpan={2}>操作</th></tr>
          <tr className={styles.metricHeadings}><th scope="col">总粉丝</th><th scope="col" title="平台作品总量">平台作品</th><th scope="col" title="本地收录量">本地收录</th></tr>
        </thead>
        <tbody>
          {!accountsReadFailed && items.map((item) => {
            const identity = item.platforms[0];
            const platformAccountLabel = platformAccountLabels[identity?.platform || ""] || "平台号";
            const rowStatus = item.account_status || "unmarked";
            const identityPending = item.directory_identity_status === "identity_missing";
            const douyinAuthorizationHref = identity?.platform === "douyin" && identity.uid && !identityPending
              ? `/accounts/douyin-authorization?account_id=${encodeURIComponent(String(item.id))}&platform_uid=${encodeURIComponent(identity.uid)}`
              : "";
            const metricTitle = identity?.data_status && identity.data_status !== "not_collected" && identity?.data_date ? `${statusLabels[identity.data_status] || identity.data_status} · 数据日期 ${formatDateTime(identity.data_date).split(" ")[0]}` : "未采集";
            return <tr key={item.id} data-account-id={item.id}>
              <th scope="row" className={styles.identityCell}>
                <div className={styles.identity}>
                  <div className={styles.identityMark}>
                    {identity?.avatar_url?.startsWith("https://") ? <><Image className={styles.avatar} src={identity.avatar_url} alt="" width={32} height={32} unoptimized /><span className={styles.platformBadge}><PlatformHeaderMark platformKey={identity.platform} /></span></> : <PlatformHeaderMark platformKey={identity?.platform || "unknown"} />}
                  </div>
                  <div className={styles.identityCopy}><strong title={identity?.nickname || "昵称缺失"}>{identity?.nickname || "昵称缺失"}</strong>
                    <div className={styles.identityMeta}>
                      <div className={styles.identityLine}><span className={styles.identifierLabel}>uid：</span><span className={styles.uid} title={`平台 UID：${identity?.uid || "平台 UID 缺失"}`}>{identity?.uid || "—"}</span>{identity?.uid && <button type="button" className={styles.copyButton} aria-label={`复制${identity.nickname || "账号"}的平台 UID`} title="复制完整平台 UID" onClick={() => { if (identity.uid) void copyAccountIdentifier(identity.uid, "平台 UID"); }}><CopyIcon aria-hidden="true" /></button>}</div>
                      <div className={styles.identityLine}><span className={styles.identifierLabel}>{platformAccountLabel}：</span><span className={styles.shortId} title={`${platformAccountLabel}：${identity?.unique_id || "—"}`}>{identity?.unique_id || "—"}</span>{identity?.unique_id && <button type="button" className={styles.copyButton} aria-label={`复制${identity.nickname || "账号"}的${platformAccountLabel}`} title={`复制完整${platformAccountLabel}`} onClick={() => { if (identity.unique_id) void copyAccountIdentifier(identity.unique_id, platformAccountLabel); }}><CopyIcon aria-hidden="true" /></button>}</div>
                    </div>
                  </div>
                </div>
              </th>
              <td><span className={styles.status} data-state={rowStatus} title={accountStatusHints[rowStatus]}><CircleIcon weight="fill" aria-hidden="true" />{accountStatusLabels[rowStatus]}</span></td>
              <td><span className={styles.phone}>{item.phone || "—"}</span></td>
              <td className={styles.metric} title={metricTitle}>{formatIdentityCount(identity?.follower_count)}</td><td className={styles.metric} title={metricTitle}>{formatIdentityCount(identity?.platform_work_count)}</td><td className={`${styles.metric} ${styles.localCount}`}>{formatIdentityCount(identity?.content_count ?? 0)}</td>
              <td>{item.operator_name || "未填写"}</td>
              <td><span className={styles.classification} title={`账号分组：${accountGroupLabel(item.account_group)}；业务方向：${businessDirectionLabel(item.business_direction)}`}>{accountGroupLabel(item.account_group)}<span aria-hidden="true"> · </span>{businessDirectionLabel(item.business_direction)}</span></td>
              <td className={styles.actionsCell}><div className={styles.actions}>
                <button type="button" className={styles.editButton} disabled={saving || displayingPrevious} aria-label={`查看${identity?.nickname || "账号"}的导入资料`} onClick={(event) => { summaryTrigger.current = event.currentTarget; void openAccountAction(item, "summary"); }}>资料</button>
                <button type="button" title={identityPending ? "可修改账号分组和业务方向" : undefined} className={styles.editButton} disabled={saving || displayingPrevious} aria-label={`修改${identity?.nickname || "账号"}的运营信息`} onClick={() => void openAccountAction(item, "edit")}>修改</button>
                {item.directory_row_id && item.locator_sha256 && (identityPending || item.account_preparation?.state === "blocked") && <button type="button" className={styles.editButton} disabled={saving || displayingPrevious} onClick={() => void openAccountAction(item, "identity")}>补充身份</button>}
                <details data-account-menu className={styles.rowMenu} onToggle={(event) => positionAccountMenu(event.currentTarget)} onBlur={(event) => { if (!event.currentTarget.contains(event.relatedTarget)) event.currentTarget.open = false; }} onKeyDown={(event) => { if (event.key === "Escape") { event.currentTarget.open = false; event.currentTarget.querySelector("summary")?.focus(); } }}>
                  <summary aria-label={`${identity?.nickname || "账号"}的更多操作`} title="更多操作"><DotsThreeVerticalIcon weight="bold" aria-hidden="true" /></summary>
                  <div data-account-menu-panel className={styles.menuPanel}>
                    {identity?.platform === "douyin" && (douyinAuthorizationHref && !displayingPrevious ? <Link href={douyinAuthorizationHref}>抖音授权</Link> : <button type="button" disabled title={displayingPrevious ? "请等待读取目标页" : "需先补充平台 UID 才能发起抖音授权"}>抖音授权</button>)}
                    {accountStatusActions.filter((action) => action.status !== rowStatus).map((action) => <button key={action.status} type="button" data-status-action={action.status} disabled={saving || displayingPrevious} onClick={(event) => {
                      const menu = event.currentTarget.closest("details");
                      if (menu) menu.open = false;
                      void changeAccountStatus(item, action.status);
                    }}>{action.label}</button>)}
                  </div>
                </details>
              </div></td>
            </tr>;
          })}
          {!accountsReadFailed && !items.length && <tr><td className={styles.empty} colSpan={9}>{appliedRequest.query || appliedRequest.account_status || appliedRequest.platform || appliedRequest.account_group || appliedRequest.business_direction ? "没有符合筛选条件的账号" : "暂无已保存账号"}</td></tr>}
        </tbody>
      </table></div>
      {accountsReadFailed && <ReadErrorState title="账号读取失败" retrying={retrying} onRetry={retryAccountsRead} />}
      {!accountsReadFailed && result && <AccountsPagination page={appliedRequest.page} pageSize={appliedRequest.page_size} total={total} busy={saving} status={readStatus} onChange={changePage} onPrefetch={prefetchPage} onCancelPrefetch={cancelPagePrefetchIntent} />}
        </div>
      </article>

    </section>}
    {summaryAccount && <AccountSummaryDialog account={summaryAccount} onClose={closeAccountSummary} />}
    {detailRead && <AccountDialogLayout id="account-detail-read" title="账号资料" description="读取当前保存的账号资料" onClose={closeDetailRead}>
      {detailRead.error ? <p role="alert">{detailRead.error}</p> : <p role="status">正在读取账号资料…</p>}
    </AccountDialogLayout>}
    {form && <AccountOperationsDialog form={form} account={editingAccount ?? undefined} onChange={setForm} onClose={() => { if (!saving) { setForm(null); setEditingAccount(null); setError(""); } }} onSave={() => void save()} saving={saving} error={error} />}
    {creatingAccount && <CreateAccountDialog accountManagementVersion={accountManagementVersion} onClose={() => setCreatingAccount(false)} onCreated={accountCreated} />}
    {identityAccount && <RepairAccountIdentityDialog account={identityAccount} onClose={() => setIdentityAccount(null)} onSaved={(response) => {
      setIdentityAccount(null); setLastIntakeId(response.intake_id ?? null); setMessage(response.message);
      void invalidateAccountData().catch(() => setError("身份已接收，列表刷新失败，请重新搜索。"));
    }} />}
    {upload && <div className="modal-backdrop" role="presentation"><section className="modal-panel operation-modal" role="dialog" aria-modal="true" aria-label="上传矩阵通完整导出"><h3>上传矩阵通官方完整导出</h3><p>{upload.name}。仅接受当前组织全部平台、全部已添加账号的官方导出，不接受统计列表或自行维护的名单。含移除时需至少10分钟后的另一份独立官方导出确认。</p><div className="modal-fields"><label>组织 / 范围<input value={exportEvidence.organization} onChange={(event) => setExportEvidence({ ...exportEvidence, organization: event.target.value })} /></label><label>官方导出时间（含时区）<input placeholder="2026-08-29T10:00:00+08:00" value={exportEvidence.exportedAt} onChange={(event) => setExportEvidence({ ...exportEvidence, exportedAt: event.target.value })} /></label><label>独立导出记录编号<input value={exportEvidence.recordId} onChange={(event) => setExportEvidence({ ...exportEvidence, recordId: event.target.value })} /></label><label>官方声明总量<input type="number" min="0" value={exportEvidence.declaredCount} onChange={(event) => setExportEvidence({ ...exportEvidence, declaredCount: event.target.value })} /></label><label>全量范围与导出来源证据<textarea value={exportEvidence.evidence} onChange={(event) => setExportEvidence({ ...exportEvidence, evidence: event.target.value })} /></label></div><p>填写说明仅作为人工证据留档，不会被视为上游 API 证明。</p><div className="modal-actions"><button className="secondary" onClick={() => setUpload(null)}>取消</button><button className="primary" disabled={saving || !exportEvidence.organization.trim() || !exportEvidence.exportedAt.trim() || !exportEvidence.recordId.trim() || !exportEvidence.declaredCount.trim() || !exportEvidence.evidence.trim()} onClick={() => void importRoster()}>{saving ? "校验中" : "上传并同步"}</button></div></section></div>}
  </AppShell>;
}
