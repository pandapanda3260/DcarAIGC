"use client";

import Image from "next/image";
import Link from "next/link";
import { Fragment, useEffect, useState } from "react";
import { BroadcastIcon, VideoCameraIcon } from "@phosphor-icons/react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppShell from "../components/AppShell";
import { Feedback, Loading, Notice } from "../components/Feedback";
import { Pagination } from "../components/Pagination";
import { apiErrorMessage, apiUrl, jsonRequest, parseCsv, readJson } from "../lib/api";
import { label, platformKeys } from "../lib/format";
import { publicAssetPath } from "../lib/paths";
import { buildAccountSearchRequest, lastPageFor } from "../lib/queryContracts";
import { accountSearchQueryOptions, douyinAuthorizationStatusesQueryOptions, queryKeys } from "../lib/queries";
import type { Account, DouyinAuthorizationStatus } from "../lib/types";
type AccountForm = {
  id: number | null; phone: string; operatorName: string; accountType: string;
  contentDirection: string; enabled: boolean;
  platforms: Record<string, { uid: string; nickname: string; realNameStatus: string }>;
};
const blankIdentity = () => ({ uid: "", nickname: "", realNameStatus: "unknown" });
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

function AccountPlatformCells({
  account,
  authorizationStatuses,
  authorizationStatusesReady,
}: {
  account: Account;
  authorizationStatuses: DouyinAuthorizationStatus[];
  authorizationStatusesReady: boolean;
}) {
  const identities = new Map(account.platforms.map((identity) => [identity.platform, identity]));
  return platformKeys.map((platformKey) => {
    const identity = identities.get(platformKey);
    const authorization = platformKey === "douyin" && identity?.uid
      ? authorizationStatuses.find((item) => item.account_id === account.id && item.platform_uid === identity.uid && item.status === "active")
      : undefined;
    const douyinUid = platformKey === "douyin" ? identity?.uid ?? "" : "";
    const needsAuthorization = Boolean(authorization && (!authorization.authorized || authorization.needs_reauthorization));
    const authorizationState = needsAuthorization ? "needs_reauthorization" : authorization?.authorized ? "authorized" : "unauthorized";
    const authorizationLabel = needsAuthorization ? "需重新授权" : authorization?.authorized ? "已授权" : "未授权";
    const hasDouyinUid = Boolean(douyinUid);
    const canOpenAuthorization = hasDouyinUid && (account.enabled || Boolean(authorization));
    const authorizationHref = canOpenAuthorization
      ? `/accounts/douyin-authorization?account_id=${encodeURIComponent(String(account.id))}&platform_uid=${encodeURIComponent(douyinUid)}`
      : "";
    return <Fragment key={platformKey}>
      <td className="account-platform-start">{identity?.uid || "—"}</td>
      <td>{identity ? label(identity.real_name_status) : "未绑定"}</td>
      {platformKey === "douyin" && <td className="account-douyin-authorization-cell">{!hasDouyinUid ? <span className="account-authorization-state unbound">未绑定</span> : !authorizationStatusesReady ? <span className="account-authorization-placeholder">—</span> : <><span className={`account-authorization-state ${authorizationState}`}>{authorizationLabel}</span>{canOpenAuthorization ? <Link href={authorizationHref}>{account.enabled && needsAuthorization ? "重新授权" : authorization ? "管理" : "去授权"}</Link> : <small>账号已停用</small>}</>}</td>}
      <td>{identity?.nickname || "—"}</td>
      <td className="account-number-cell">{formatIdentityCount(identity?.follower_count)}</td>
      <td className="account-number-cell">{formatIdentityCount(identity ? identity.content_count : 0, " 条")}</td>
    </Fragment>;
  });
}

function PlatformHeaderMark({ platformKey }: { platformKey: string }) {
  let mark = <VideoCameraIcon weight="fill" />;
  if (platformKey === "douyin") mark = <Image src={publicAssetPath("/brand-douyin-tiktok.svg")} alt="" width={10} height={10} unoptimized />;
  if (platformKey === "xiaohongshu") mark = <Image src={publicAssetPath("/brand-xiaohongshu.svg")} alt="" width={10} height={10} unoptimized />;
  if (platformKey === "wechat_channels") mark = <BroadcastIcon weight="fill" />;
  return <span className="account-platform-icon" data-platform-mark={platformKey} aria-hidden="true">{mark}</span>;
}

function emptyForm(): AccountForm {
  return { id: null, phone: "", operatorName: "", accountType: "unknown", contentDirection: "unknown", enabled: true,
    platforms: Object.fromEntries(platformKeys.map((key) => [key, blankIdentity()])),
  };
}

export default function AccountsPage() {
  const [query, setQuery] = useState("");
  const [accountType, setAccountType] = useState("");
  const [direction, setDirection] = useState("");
  const [platform, setPlatform] = useState("");
  const [appliedRequest, setAppliedRequest] = useState(() => buildAccountSearchRequest({
    query: "", accountType: "", direction: "", platform: "",
  }, 1, 50));
  const [form, setForm] = useState<AccountForm | null>(null);
  const [saving, setSaving] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const queryClient = useQueryClient();
  const accountsQuery = useQuery(accountSearchQueryOptions(appliedRequest));
  const douyinAuthorizationStatusesQuery = useQuery(douyinAuthorizationStatusesQueryOptions());
  const result = accountsQuery.data;
  const items = result?.items ?? [];
  const pending = result?.pending_platform_identities ?? [];
  const pendingCount = result?.pending_platform_identity_count ?? 0;
  const unassociated = result?.legacy_unassociated_content_count ?? 0;
  const total = result?.total ?? 0;
  const accountsReadFailed = accountsQuery.isLoadingError || retrying;
  const douyinAuthorizationStatuses = douyinAuthorizationStatusesQuery.data?.items ?? [];

  function retryAccountsRead() {
    if (retrying) return;
    setRetrying(true);
    void accountsQuery.refetch().finally(() => setRetrying(false));
  }

  function applySearch(overrides: Partial<{ query: string; accountType: string; direction: string; platform: string; page: number; pageSize: number }> = {}) {
    const filters = { query, accountType, direction, platform, ...overrides };
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

  useEffect(() => {
    const refreshAuthorizationStatuses = () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.authorizationStatuses, exact: true });
    };
    const receivesAuthorizationUpdate = (data: unknown) => (
      typeof data === "object"
      && data !== null
      && "type" in data
      && data.type === "dcar-douyin-authorization-updated"
    );
    const handleWindowMessage = (event: MessageEvent) => {
      if (event.origin !== window.location.origin || !receivesAuthorizationUpdate(event.data)) return;
      refreshAuthorizationStatuses();
    };
    let channel: BroadcastChannel | null = null;
    if ("BroadcastChannel" in window) {
      channel = new BroadcastChannel("dcar-douyin-authorization");
      channel.addEventListener("message", (event) => {
        if (receivesAuthorizationUpdate(event.data)) refreshAuthorizationStatuses();
      });
    }
    window.addEventListener("message", handleWindowMessage);
    return () => {
      channel?.close();
      window.removeEventListener("message", handleWindowMessage);
    };
  }, [queryClient]);

  async function invalidateAccountData() {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.accounts }),
      queryClient.invalidateQueries({ queryKey: queryKeys.contents }),
      queryClient.invalidateQueries({ queryKey: queryKeys.overview }),
      queryClient.invalidateQueries({ queryKey: queryKeys.activeSellingPoints, exact: true }),
    ]);
  }

  function edit(account?: Account) {
    if (!account) { setForm(emptyForm()); return; }
    const next = emptyForm();
    Object.assign(next, { id: account.id, phone: account.phone, operatorName: account.operator_name, accountType: account.account_type, contentDirection: account.content_direction, enabled: account.enabled });
    account.platforms.forEach((identity) => { next.platforms[identity.platform] = { uid: identity.uid, nickname: identity.nickname, realNameStatus: identity.real_name_status }; });
    setForm(next);
  }

  async function save() {
    if (!form) return;
    setSaving(true); setError(""); setMessage("");
    try {
      const body = { phone: form.phone, operator_name: form.operatorName, account_type: form.accountType, content_direction: form.contentDirection, enabled: form.enabled,
        platforms: platformKeys.filter((key) => form.platforms[key].uid.trim()).map((key) => ({ platform: key, uid: form.platforms[key].uid.trim(), nickname: form.platforms[key].nickname, real_name_status: form.platforms[key].realNameStatus })),
      };
      await readJson(form.id ? `/api/v8/accounts/${form.id}` : "/api/v8/accounts", jsonRequest(body, form.id ? "PATCH" : "POST"));
      const wasEdit = Boolean(form.id); setForm(null); await invalidateAccountData(); setMessage(wasEdit ? "账号已更新" : "账号已新增");
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号保存失败"); }
    finally { setSaving(false); }
  }

  async function importCsv(file: File) {
    setSaving(true); setError(""); setMessage("");
    try {
      const rows = parseCsv(await file.text());
      if (!rows.length) throw new Error("文件中没有可导入的数据。");
      const result = await readJson<{ inserted_rows: number; updated_rows: number; rejected_rows: number }>("/api/v8/accounts/import", jsonRequest({ source_name: file.name, rows }));
      await invalidateAccountData(); setMessage(`导入完成：新增 ${result.inserted_rows} 条，更新 ${result.updated_rows} 条，${result.rejected_rows} 条未导入。`);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号导入失败"); }
    finally { setSaving(false); }
  }

  async function exportWorkbook() {
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
        response = await fetch(apiUrl("/api/v8/accounts/export"), jsonRequest({ douyin_authorization_targets: authorizationTargets }));
      } catch {
        throw new Error("无法连接数据服务，请检查网络或稍后重试。");
      }
      if (!response.ok) {
        const body = (await response.json().catch(() => null)) as { detail?: unknown } | null;
        throw new Error(apiErrorMessage(body?.detail, response.status));
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

  return <AppShell active="accounts">
    <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {accountsQuery.isError && <Notice tone="error">{accountsQuery.data ? `数据刷新失败，当前显示上次数据。${accountsQuery.error instanceof Error ? accountsQuery.error.message : ""}` : accountsQuery.error instanceof Error ? accountsQuery.error.message : "账号读取失败"}</Notice>}
    {douyinAuthorizationStatusesQuery.isError && <Notice tone="error">{douyinAuthorizationStatusesQuery.data && !douyinAuthorizationStatusesQuery.data.unavailable ? "抖音开平授权状态刷新失败，当前显示上次状态。" : "抖音开平授权状态读取失败，列表暂以“—”显示。"}</Notice>}
    {accountsQuery.isPending && !accountsQuery.data && !accountsReadFailed ? <Loading label="正在读取账号库" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">手机号是唯一账号标识</span><h2>账号信息</h2><p>每个手机号对应一个账号；还没采集到粉丝量时显示“—”。</p></div><div className="placeholder-actions"><button className="primary small" onClick={() => edit()}>新增账号</button><Link className="secondary button-link" href="/accounts/douyin-authorization">抖音授权管理</Link><button className="secondary button-link" disabled={exporting} onClick={() => void exportWorkbook()}>{exporting ? "正在导出…" : "下载账号表格"}</button><label className="secondary button-link">批量导入<input className="file-input" type="file" accept=".csv,text/csv" disabled={saving} onChange={(event) => { const file = event.target.files?.[0]; if (file) void importCsv(file); event.currentTarget.value = ""; }} /></label></div></div>
      <div className="filter-bar"><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="手机号、运营人员、平台账号编号、昵称" onKeyDown={(event) => { if (event.key === "Enter") applySearch({ page: 1 }); }} /><select value={accountType} onChange={(event) => { setAccountType(event.target.value); applySearch({ accountType: event.target.value, page: 1 }); }}><option value="">全部账号类型</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option><option value="unknown">未知</option></select><select value={direction} onChange={(event) => { setDirection(event.target.value); applySearch({ direction: event.target.value, page: 1 }); }}><option value="">全部内容方向</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option><option value="unknown">未知</option></select><select value={platform} onChange={(event) => { setPlatform(event.target.value); applySearch({ platform: event.target.value, page: 1 }); }}><option value="">全部平台</option>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select><button className="secondary" onClick={() => applySearch({ page: 1 })}>搜索</button><span>{accountsReadFailed ? "读取失败" : `${total} 个账号`}</span></div>
      <article className="panel table-panel account-master-panel">
        {!accountsReadFailed && accountsQuery.data && <Pagination page={appliedRequest.page} pageSize={appliedRequest.page_size} total={total} busy={accountsQuery.isFetching || saving} ariaLabel="账号分页" unitLabel="个账号" placement="top" onChange={(next) => applySearch({ page: next.page, pageSize: next.pageSize })} />}
        <div className="table-scroll"><table className="account-master-table">
        <caption className="visually-hidden">以手机号为唯一标识的账号列表</caption>
        <colgroup>
          <col className="account-phone-col" /><col className="account-operator-col" /><col className="account-type-col" /><col className="account-direction-col" />
        </colgroup>
        {platformKeys.map((key) => <colgroup key={key} data-platform-columns={key}>
          <col className="account-uid-col" /><col className="account-realname-col" />{key === "douyin" && <col className="account-douyin-authorization-col" />}<col className="account-nickname-col" /><col className="account-followers-col" /><col className="account-content-count-col" />
        </colgroup>)}
        <colgroup>
          <col className="account-status-col" /><col className="account-action-col" />
        </colgroup>
        <thead>
          <tr className="account-group-row">
            <th className="account-base-group" colSpan={4} scope="colgroup">账号基础信息</th>
            {platformKeys.map((key) => <th className="account-platform-group" data-platform={key} colSpan={key === "douyin" ? 6 : 5} scope="colgroup" key={key}><span className="account-platform-heading"><PlatformHeaderMark platformKey={key} />{label(key)}</span></th>)}
            <th className="account-management-group" colSpan={2} scope="colgroup">账号管理</th>
          </tr>
          <tr className="account-column-row"><th className="account-sticky account-phone" scope="col">手机号</th><th className="account-sticky account-operator" scope="col">运营人员</th><th className="account-sticky account-type" scope="col">账号类型</th><th className="account-sticky account-direction" scope="col">内容方向</th>{platformKeys.map((key) => <Fragment key={key}><th className="account-platform-start" scope="col">平台账号编号</th><th scope="col">是否实名</th>{key === "douyin" && <th scope="col">抖音开平授权</th>}<th scope="col">昵称</th><th className="account-number-cell" scope="col">粉丝量</th><th className="account-number-cell" scope="col">关联内容量</th></Fragment>)}<th scope="col">状态</th><th scope="col">操作</th></tr>
        </thead>
        <tbody>
          {accountsReadFailed && <tr><td className="table-read-error" colSpan={27}><strong>账号读取失败</strong><span>请检查网络后重新加载。</span><button type="button" className="secondary read-error-retry" disabled={retrying} onClick={retryAccountsRead}>{retrying ? "正在重新加载…" : "重新加载"}</button></td></tr>}
          {!accountsReadFailed && items.map((item) => <tr key={item.id}><th className="account-sticky account-phone" scope="row"><strong>{item.phone}</strong></th><td className="account-sticky account-operator">{item.operator_name || "未填写"}</td><td className="account-sticky account-type">{label(item.account_type)}</td><td className="account-sticky account-direction">{label(item.content_direction)}</td><AccountPlatformCells account={item} authorizationStatuses={douyinAuthorizationStatuses} authorizationStatusesReady={Boolean(douyinAuthorizationStatusesQuery.data && !douyinAuthorizationStatusesQuery.data.unavailable)} /><td>{item.enabled ? "运营中" : "停用"}</td><td><button className="text-button" onClick={() => edit(item)}>修改</button></td></tr>)}
          {!accountsReadFailed && !items.length && <tr><td className="account-master-empty" colSpan={27}>暂无账号，请新增账号或批量导入</td></tr>}
        </tbody>
      </table></div></article>
      {!accountsReadFailed && accountsQuery.data && <article className="panel pending-identity-panel">
        <div className="panel-head"><div><span className="eyebrow">等待匹配</span><h3>待匹配的平台账号 · {pendingCount}</h3><p>还有 {unassociated} 条已有内容没有关联手机号。新增或导入账号并匹配平台账号编号后，系统会自动关联。</p></div></div>
        <div className="pending-identity-list table-scroll">
          <table className="pending-identity-table">
            <caption className="visually-hidden">待匹配的平台账号列表</caption>
            <thead><tr><th>平台</th><th>平台账号编号</th><th>昵称</th><th>关联内容</th></tr></thead>
            <tbody>
              {pending.map((item) => <tr key={`${item.platform}:${item.uid}`}><td><span className="pending-platform-label">{label(item.platform)}</span></td><td><strong>{item.uid}</strong></td><td>{item.nickname || "昵称缺失"}</td><td>{item.content_count} 条</td></tr>)}
              {!pending.length && <tr><td className="pending-identity-empty" colSpan={4}>没有待匹配的平台账号</td></tr>}
            </tbody>
          </table>
        </div>
      </article>}
    </section>}
    {form && <div className="modal-backdrop" role="presentation"><section className="modal-panel operation-modal" role="dialog" aria-modal="true" aria-label="编辑账号"><div className="panel-head"><div><span className="eyebrow">账号信息</span><h3>{form.id ? "修改账号" : "新增账号"}</h3></div><button className="modal-close" onClick={() => setForm(null)} aria-label="关闭">×</button></div><div className="modal-fields"><label>手机号<input value={form.phone} onChange={(event) => setForm({ ...form, phone: event.target.value })} /></label><label>运营人员<input value={form.operatorName} onChange={(event) => setForm({ ...form, operatorName: event.target.value })} /></label><label>账号类型<select value={form.accountType} onChange={(event) => setForm({ ...form, accountType: event.target.value })}><option value="unknown">未知</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option></select></label><label>内容方向<select value={form.contentDirection} onChange={(event) => setForm({ ...form, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label><label className="toggle-field"><input type="checkbox" checked={form.enabled} onChange={(event) => setForm({ ...form, enabled: event.target.checked })} />运营中</label></div><div className="platform-editor">{platformKeys.map((key) => <fieldset key={key}><legend>{label(key)}</legend><label>平台账号编号<input value={form.platforms[key].uid} onChange={(event) => setForm({ ...form, platforms: { ...form.platforms, [key]: { ...form.platforms[key], uid: event.target.value } } })} /></label><label>实名<select value={form.platforms[key].realNameStatus} onChange={(event) => setForm({ ...form, platforms: { ...form.platforms, [key]: { ...form.platforms[key], realNameStatus: event.target.value } } })}><option value="unknown">未知</option><option value="yes">是</option><option value="no">否</option></select></label><label>昵称<input value={form.platforms[key].nickname} onChange={(event) => setForm({ ...form, platforms: { ...form.platforms, [key]: { ...form.platforms[key], nickname: event.target.value } } })} /></label></fieldset>)}</div><div className="modal-actions"><button className="secondary" onClick={() => setForm(null)}>取消</button><button className="primary" disabled={saving} onClick={() => void save()}>{saving ? "保存中" : "保存账号"}</button></div></section></div>}
  </AppShell>;
}
