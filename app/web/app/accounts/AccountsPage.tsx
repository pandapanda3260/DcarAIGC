"use client";

import Image from "next/image";
import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { CircleIcon, CopyIcon, DotsThreeVerticalIcon, VideoCameraIcon } from "@phosphor-icons/react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppShell from "../components/AppShell";
import AccountPageAccess from "../components/AccountPageAccess";
import { Feedback, Loading, Notice, ReadErrorState } from "../components/Feedback";
import { AccountsPagination } from "./AccountsPagination";
import CreateAccountDialog, { type CreateAccountResponse } from "./CreateAccountDialog";
import { apiErrorMessage, apiUrl, handleApprovalRequired, jsonRequest, readJson } from "../lib/api";
import { PLATFORM_LOGO_PATHS } from "../lib/contentMedia";
import { formatDateTime, label, platformKeys } from "../lib/format";
import { publicAssetPath } from "../lib/paths";
import styles from "./accounts.module.css";
import { buildAccountSearchRequest, lastPageFor } from "../lib/queryContracts";
import { accountSearchQueryOptions, defaultAccountSearchRequest, douyinAuthorizationStatusesQueryOptions, queryKeys } from "../lib/queries";
import type { Account, AccountStatus } from "../lib/types";
type EditableAccountStatus = Exclude<AccountStatus, "unmarked"> | "";
type AccountForm = {
  id: number; phone: string; operatorName: string; accountType: string;
  contentDirection: string; accountStatus: EditableAccountStatus;
  originalAccountStatus: EditableAccountStatus;
};
const statusLabels: Record<string, string> = {
  monitored: "已监测", not_monitored: "未监测", authorized: "已授权",
  unauthorized: "未授权", unknown: "未知", not_collected: "未采集",
  stale: "待更新", available: "已更新",
};
const accountStatusLabels: Record<AccountStatus, string> = {
  daily: "日更", weekly: "周更", paused: "暂停", unmarked: "待标记",
};
const accountStatusHints: Record<AccountStatus, string> = {
  daily: "人工标记为每天更新作品，采集规则不变。",
  weekly: "人工标记为每周更新作品，采集规则不变。",
  paused: "已停止采集并退出当前生效名单，相关数据不进入统计。",
  unmarked: "尚未标记作品更新频率，采集规则保持不变。",
};
const integerFormat = new Intl.NumberFormat("zh-CN");

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
  const [accountType, setAccountType] = useState("");
  const [accountStatus, setAccountStatus] = useState<AccountStatus | "">("");
  const [direction, setDirection] = useState("");
  const [platform, setPlatform] = useState("");
  const [upload, setUpload] = useState<File | null>(null);
  const [exportEvidence, setExportEvidence] = useState({ organization: "", exportedAt: "", recordId: "", declaredCount: "", evidence: "" });
  const [appliedRequest, setAppliedRequest] = useState(() => ({ ...defaultAccountSearchRequest }));
  const [form, setForm] = useState<AccountForm | null>(null);
  const [creatingAccount, setCreatingAccount] = useState(false);
  const [saving, setSaving] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const statusRequests = useRef(new Map<string, string>());
  const queryClient = useQueryClient();
  const accountsQuery = useQuery(accountSearchQueryOptions(appliedRequest));
  const result = accountsQuery.data;
  const items = result?.items ?? [];
  const total = result?.total ?? 0;
  const accountManagementVersion = result?.account_management_version ?? 1;
  const exportUnavailable = accountManagementVersion < 2 && Boolean(appliedRequest.account_status);
  const exportUnavailableMessage = "服务更新完成后可按账号状态导出。";
  const managedMode = result?.roster?.source_family === "system" && result.roster.active_profile_id != null;
  const accountsReadFailed = accountsQuery.isLoadingError || retrying;

  function retryAccountsRead() {
    if (retrying) return;
    setRetrying(true);
    void accountsQuery.refetch().finally(() => setRetrying(false));
  }

  function applySearch(overrides: Partial<{ query: string; accountType: string; accountStatus: AccountStatus | ""; direction: string; platform: string; page: number; pageSize: number }> = {}) {
    const filters = { query, accountType, accountStatus, direction, platform, ...overrides };
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

  useEffect(() => {
    if (!result || accountsQuery.isPlaceholderData) return;
    const lastPage = lastPageFor(result.total, appliedRequest.page_size);
    if (appliedRequest.page > lastPage) {
      const timer = window.setTimeout(() => {
        setAppliedRequest((current) => ({ ...current, page: lastPage }));
      }, 0);
      return () => window.clearTimeout(timer);
    }
  }, [accountsQuery.isPlaceholderData, appliedRequest.page, appliedRequest.page_size, result]);

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
    const status = account.account_status && account.account_status !== "unmarked" ? account.account_status : "";
    setForm({ id: account.id, phone: account.phone, operatorName: account.operator_name, accountType: account.account_type, contentDirection: account.content_direction, accountStatus: status, originalAccountStatus: status });
  }

  async function copyUid(uid: string) {
    try {
      await navigator.clipboard.writeText(uid);
      setMessage("平台 UID 已复制");
    } catch {
      setError("复制失败，请在账号编号提示中查看完整 UID 后手动复制。");
    }
  }

  async function save() {
    if (!form) return;
    setSaving(true); setError(""); setMessage("");
    try {
      const body = {
        phone: form.phone, operator_name: form.operatorName, account_type: form.accountType, content_direction: form.contentDirection,
        ...(form.accountStatus && form.accountStatus !== form.originalAccountStatus ? { account_status: form.accountStatus } : {}),
      };
      const requestKey = `save:${form.id}:${JSON.stringify(body)}`;
      if (body.account_status && !statusRequests.current.has(requestKey)) statusRequests.current.set(requestKey, crypto.randomUUID());
      const response = await readJson<{ message: string }>(`/api/v8/accounts/${form.id}`, jsonRequest({
        ...body,
        ...(body.account_status ? { status_request_id: statusRequests.current.get(requestKey) } : {}),
      }, "PATCH"));
      statusRequests.current.delete(requestKey);
      setForm(null); await invalidateAccountData(); setMessage(response.message);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号保存失败"); }
    finally { setSaving(false); }
  }

  function accountCreated(response: CreateAccountResponse) {
    setCreatingAccount(false);
    setQuery(response.uid);
    setAccountType("");
    setAccountStatus("");
    setDirection("");
    setPlatform("");
    applySearch({ query: response.uid, accountType: "", accountStatus: "", direction: "", platform: "", page: 1 });
    setMessage(response.message);
    void invalidateAccountData().catch(() => setError("账号已添加，但列表刷新失败，请重新搜索。"));
  }

  async function pauseManagedAccount(account: Account) {
    if (saving || account.account_status === "paused") return;
    const identity = account.platforms[0];
    if (!window.confirm(`确认暂停 ${identity?.nickname || identity?.uid || "该账号"}？暂停后将停止采集、退出当前生效名单，相关数据不进入统计；历史数据保留。`)) return;
    setSaving(true); setError(""); setMessage("");
    try {
      const requestKey = `pause:${account.id}`;
      if (!statusRequests.current.has(requestKey)) statusRequests.current.set(requestKey, crypto.randomUUID());
      const response = await readJson<{ message: string }>(`/api/v8/accounts/${account.id}`, jsonRequest({
        account_status: "paused", status_request_id: statusRequests.current.get(requestKey),
      }, "PATCH"));
      statusRequests.current.delete(requestKey);
      await invalidateAccountData(); setMessage(response.message);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号暂停失败"); }
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
        response = await fetch(apiUrl("/api/v8/accounts/export"), jsonRequest({ douyin_authorization_targets: authorizationTargets, ...(accountManagementVersion >= 2 ? { account_status: appliedRequest.account_status } : { scope: "all" }) }));
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

  return <AppShell active="accounts" header={accountsQuery.isPending && !accountsQuery.data && !accountsReadFailed ? undefined :
    <header className="page-header"><div className="page-header-copy"><span className="page-header-eyebrow">{managedMode ? "系统托管名单" : "矩阵通名册"}</span><h1 className="page-header-title">账号信息</h1><p className="page-header-description">一个平台账号一行，手机号仅作运营信息；未采集的粉丝和平台作品总量显示“—”。</p></div><div className="page-header-actions">{managedMode ? <button className="primary" disabled={saving} onClick={() => { setError(""); setMessage(""); setCreatingAccount(true); }}>新增系统账号</button> : <label className="secondary button-link">批量上传账号<input className="file-input" type="file" accept=".xlsx,.csv,.json" disabled={saving} onChange={(event) => { const file = event.target.files?.[0]; if (file) setUpload(file); event.currentTarget.value = ""; }} /></label>}<button className="secondary button-link" disabled={exporting || exportUnavailable} title={exportUnavailable ? exportUnavailableMessage : undefined} onClick={() => void exportWorkbook()}>{exporting ? "正在导出…" : "下载账号表格"}</button></div></header>
  }>
    <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {accountsQuery.isError && <Notice tone="error">{accountsQuery.data ? `数据刷新失败，当前显示上次数据。${accountsQuery.error instanceof Error ? accountsQuery.error.message : ""}` : accountsQuery.error instanceof Error ? accountsQuery.error.message : "账号读取失败"}</Notice>}
    {accountsQuery.isPending && !accountsQuery.data && !accountsReadFailed ? <Loading label="正在读取账号库" /> : <section className="page-stack wide-stack">
      <div className="filter-bar"><select aria-label="账号状态筛选" value={accountStatus} onChange={(event) => { const nextStatus = event.target.value as AccountStatus | ""; setAccountStatus(nextStatus); applySearch({ accountStatus: nextStatus, page: 1 }); }}><option value="">全部账号状态</option><option value="daily">日更</option><option value="weekly">周更</option><option value="paused">暂停</option><option value="unmarked">待标记</option></select><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="手机号、运营人员、平台账号编号、昵称" onKeyDown={(event) => { if (event.key === "Enter") applySearch({ page: 1 }); }} /><select value={accountType} onChange={(event) => { setAccountType(event.target.value); applySearch({ accountType: event.target.value, page: 1 }); }}><option value="">全部账号类型</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option><option value="unknown">未知</option></select><select value={direction} onChange={(event) => { setDirection(event.target.value); applySearch({ direction: event.target.value, page: 1 }); }}><option value="">全部内容方向</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option><option value="unknown">未知</option></select><select value={platform} onChange={(event) => { setPlatform(event.target.value); applySearch({ platform: event.target.value, page: 1 }); }}><option value="">全部平台</option>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select><button className="secondary" onClick={() => applySearch({ page: 1 })}>搜索</button><span>{accountsReadFailed ? "读取失败" : `${total} 个账号`}</span></div>
      {exportUnavailable && <p role="status">{exportUnavailableMessage}</p>}
      <article>
        {!accountsReadFailed && <div className={styles.tableTitle}><h2>账号列表</h2><span>共 {total} 个账号</span></div>}
        <div className={`${styles.accountPanel}${accountsReadFailed ? " has-read-error" : ""}`}>
        <div className={`table-scroll ${styles.tableScroll}`}><table className={styles.memberTable}>
        <caption className="visually-hidden">{managedMode ? "系统托管账号名单" : "矩阵通账号名单"}：一个平台账号一行</caption>
        <colgroup><col className={styles.accountColumn} /><col className={styles.rosterColumn} /><col className={styles.phoneColumn} /><col className={styles.metricColumn} /><col className={styles.metricColumn} /><col className={styles.metricColumn} /><col className={styles.operatorColumn} /><col className={styles.classificationColumn} /><col className={styles.actionsColumn} /></colgroup>
        <thead>
          <tr><th scope="col" rowSpan={2} className={styles.accountHeading}>账号</th><th scope="col" rowSpan={2}>账号状态</th><th scope="col" rowSpan={2}>手机号</th><th scope="colgroup" colSpan={3}>数据规模</th><th scope="col" rowSpan={2}>运营人员</th><th scope="col" rowSpan={2}>账号分类</th><th scope="col" rowSpan={2}>操作</th></tr>
          <tr className={styles.metricHeadings}><th scope="col">总粉丝</th><th scope="col" title="平台作品总量">平台作品</th><th scope="col" title="本地收录量">本地收录</th></tr>
        </thead>
        <tbody>
          {!accountsReadFailed && items.map((item) => {
            const identity = item.platforms[0];
            const rowStatus = item.account_status || "unmarked";
            const canPause = managedMode && rowStatus !== "paused";
            const douyinAuthorizationHref = identity?.platform === "douyin" && identity.uid ? `/accounts/douyin-authorization?account_id=${encodeURIComponent(String(item.id))}&platform_uid=${encodeURIComponent(identity.uid)}` : "";
            const metricTitle = identity?.data_status && identity.data_status !== "not_collected" && identity?.data_date ? `${statusLabels[identity.data_status] || identity.data_status} · 数据日期 ${formatDateTime(identity.data_date).split(" ")[0]}` : "未采集";
            return <tr key={item.id} data-account-id={item.id}>
              <th scope="row" className={styles.identityCell}>
                <div className={styles.identity}>
                  <div className={styles.identityMark}>
                    {identity?.avatar_url?.startsWith("https://") ? <><Image className={styles.avatar} src={identity.avatar_url} alt="" width={32} height={32} unoptimized /><span className={styles.platformBadge}><PlatformHeaderMark platformKey={identity.platform} /></span></> : <PlatformHeaderMark platformKey={identity?.platform || "unknown"} />}
                  </div>
                  <div className={styles.identityCopy}><strong title={identity?.nickname || "昵称缺失"}>{identity?.nickname || "昵称缺失"}</strong>
                    <div className={styles.identityMeta}><span className={styles.uid} title={`平台 UID：${identity?.uid || "平台 UID 缺失"}\n短号：${identity?.unique_id || "—"}`}>{identity?.uid || "平台 UID 缺失"}</span>{identity?.unique_id && <><span aria-hidden="true">·</span><span className={styles.shortId} title={`短号：${identity.unique_id}`}>{identity.unique_id}</span></>}{identity?.uid && <button type="button" className={styles.copyButton} aria-label={`复制${identity.nickname || "账号"}的平台 UID`} title="复制完整平台 UID" onClick={() => { if (identity.uid) void copyUid(identity.uid); }}><CopyIcon aria-hidden="true" /></button>}</div>
                  </div>
                </div>
              </th>
              <td><div className={styles.cellStack}><span className={styles.status} data-state={rowStatus} title={accountStatusHints[rowStatus]}><CircleIcon weight="fill" aria-hidden="true" />{accountStatusLabels[rowStatus]}</span></div></td>
              <td><span className={styles.phone}>{item.phone || "—"}</span></td>
              <td className={styles.metric} title={metricTitle}>{formatIdentityCount(identity?.follower_count)}</td><td className={styles.metric} title={metricTitle}>{formatIdentityCount(identity?.platform_work_count)}</td><td className={`${styles.metric} ${styles.localCount}`}>{formatIdentityCount(identity?.content_count ?? 0)}</td>
              <td>{item.operator_name || "未填写"}</td>
              <td><span className={styles.classification} title={`账号类型：${label(item.account_type)}；内容方向：${label(item.content_direction)}`}>{label(item.account_type)}<span aria-hidden="true"> · </span>{label(item.content_direction)}</span></td>
              <td><div className={styles.actions}><button type="button" className={styles.editButton} aria-label={`修改${identity?.nickname || "账号"}的运营信息`} onClick={() => edit(item)}>修改</button>{(douyinAuthorizationHref || canPause) && <details className={styles.rowMenu} onBlur={(event) => { if (!event.currentTarget.contains(event.relatedTarget)) event.currentTarget.open = false; }} onKeyDown={(event) => { if (event.key === "Escape") { event.currentTarget.open = false; event.currentTarget.querySelector("summary")?.focus(); } }}><summary aria-label={`${identity?.nickname || "账号"}的更多操作`} title="更多操作"><DotsThreeVerticalIcon weight="bold" aria-hidden="true" /></summary><div className={styles.menuPanel}>{douyinAuthorizationHref && <Link href={douyinAuthorizationHref}>抖音授权</Link>}{canPause && <button type="button" disabled={saving} onClick={(event) => { const menu = event.currentTarget.closest("details"); if (menu) menu.open = false; void pauseManagedAccount(item); }}>暂停账号</button>}</div></details>}</div></td>
            </tr>;
          })}
          {!accountsReadFailed && !items.length && <tr><td className={styles.empty} colSpan={9}>{appliedRequest.query || appliedRequest.account_status || appliedRequest.platform || appliedRequest.account_type || appliedRequest.content_direction ? "没有符合筛选条件的账号" : "暂无已保存账号"}</td></tr>}
        </tbody>
      </table></div>
      {accountsReadFailed && <ReadErrorState title="账号读取失败" retrying={retrying} onRetry={retryAccountsRead} />}
      {!accountsReadFailed && accountsQuery.data && <AccountsPagination page={appliedRequest.page} pageSize={appliedRequest.page_size} total={total} busy={accountsQuery.isFetching || saving} onChange={(next) => applySearch({ page: next.page, pageSize: next.pageSize })} />}
        </div>
      </article>

    </section>}
    {form && <div className="modal-backdrop" role="presentation"><section className="modal-panel operation-modal" role="dialog" aria-modal="true" aria-label="编辑账号"><div className="panel-head"><h3>修改账号运营信息</h3><button className="modal-close" onClick={() => setForm(null)} aria-label="关闭">×</button></div><p>可修改账号状态和运营信息。手机号可留空，也可以由多个账号共用。</p><div className="modal-fields"><label>手机号（可留空）<input value={form.phone} onChange={(event) => setForm({ ...form, phone: event.target.value })} /></label><label>运营人员<input value={form.operatorName} onChange={(event) => setForm({ ...form, operatorName: event.target.value })} /></label><label>账号类型<select value={form.accountType} onChange={(event) => setForm({ ...form, accountType: event.target.value })}><option value="unknown">未知</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option></select></label><label>内容方向<select value={form.contentDirection} onChange={(event) => setForm({ ...form, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label><label>账号状态<select value={form.accountStatus} aria-describedby="account-status-help" onChange={(event) => setForm({ ...form, accountStatus: event.target.value as EditableAccountStatus })}>{!form.originalAccountStatus && <option value="" disabled>待标记</option>}<option value="daily">日更</option><option value="weekly">周更</option><option value="paused">暂停</option></select><span id="account-status-help" className={styles.statusHelp}>日更、周更仅标注作品更新频率，采集规则不变。暂停将停止采集、退出当前生效名单，相关数据不进入统计；历史数据保留。</span></label></div><div className="modal-actions"><button className="secondary" onClick={() => setForm(null)}>取消</button><button className="primary" disabled={saving} onClick={() => void save()}>{saving ? "保存中" : "保存修改"}</button></div></section></div>}
    {creatingAccount && <CreateAccountDialog accountManagementVersion={accountManagementVersion} onClose={() => setCreatingAccount(false)} onCreated={accountCreated} />}
    {upload && <div className="modal-backdrop" role="presentation"><section className="modal-panel operation-modal" role="dialog" aria-modal="true" aria-label="上传矩阵通完整导出"><h3>上传矩阵通官方完整导出</h3><p>{upload.name}。仅接受当前组织全部平台、全部已添加账号的官方导出，不接受统计列表或自行维护的名单。含移除时需至少10分钟后的另一份独立官方导出确认。</p><div className="modal-fields"><label>组织 / 范围<input value={exportEvidence.organization} onChange={(event) => setExportEvidence({ ...exportEvidence, organization: event.target.value })} /></label><label>官方导出时间（含时区）<input placeholder="2026-08-29T10:00:00+08:00" value={exportEvidence.exportedAt} onChange={(event) => setExportEvidence({ ...exportEvidence, exportedAt: event.target.value })} /></label><label>独立导出记录编号<input value={exportEvidence.recordId} onChange={(event) => setExportEvidence({ ...exportEvidence, recordId: event.target.value })} /></label><label>官方声明总量<input type="number" min="0" value={exportEvidence.declaredCount} onChange={(event) => setExportEvidence({ ...exportEvidence, declaredCount: event.target.value })} /></label><label>全量范围与导出来源证据<textarea value={exportEvidence.evidence} onChange={(event) => setExportEvidence({ ...exportEvidence, evidence: event.target.value })} /></label></div><p>填写说明仅作为人工证据留档，不会被视为上游 API 证明。</p><div className="modal-actions"><button className="secondary" onClick={() => setUpload(null)}>取消</button><button className="primary" disabled={saving || !exportEvidence.organization.trim() || !exportEvidence.exportedAt.trim() || !exportEvidence.recordId.trim() || !exportEvidence.declaredCount.trim() || !exportEvidence.evidence.trim()} onClick={() => void importRoster()}>{saving ? "校验中" : "上传并同步"}</button></div></section></div>}
  </AppShell>;
}
