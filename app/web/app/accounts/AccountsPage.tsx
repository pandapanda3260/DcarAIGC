"use client";

import Image from "next/image";
import { Fragment, useEffect, useState } from "react";
import { BroadcastIcon, VideoCameraIcon } from "@phosphor-icons/react";
import AppShell from "../components/AppShell";
import { Feedback, Loading } from "../components/Feedback";
import { Pagination } from "../components/Pagination";
import { API_BASE, jsonRequest, parseCsv, readJson } from "../lib/api";
import { label, platformKeys } from "../lib/format";
import { publicAssetPath } from "../lib/paths";
import type { Account, PendingPlatformIdentity } from "../lib/types";

type AccountSearchResult = {
  items: Account[]; total: number; legacy_unassociated_content_count: number;
  pending_platform_identity_count: number; pending_platform_identities: PendingPlatformIdentity[];
};
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

function AccountPlatformCells({ account }: { account: Account }) {
  const identities = new Map(account.platforms.map((identity) => [identity.platform, identity]));
  return platformKeys.map((platformKey) => {
    const identity = identities.get(platformKey);
    return <Fragment key={platformKey}>
      <td className="account-platform-start">{identity?.uid || "—"}</td>
      <td>{identity ? label(identity.real_name_status) : "未绑定"}</td>
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
  const [items, setItems] = useState<Account[]>([]);
  const [pending, setPending] = useState<PendingPlatformIdentity[]>([]);
  const [pendingCount, setPendingCount] = useState(0);
  const [unassociated, setUnassociated] = useState(0);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [fetching, setFetching] = useState(false);
  const [query, setQuery] = useState("");
  const [accountType, setAccountType] = useState("");
  const [direction, setDirection] = useState("");
  const [platform, setPlatform] = useState("");
  const [form, setForm] = useState<AccountForm | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  async function reload(overrides: Partial<{ query: string; accountType: string; direction: string; platform: string; page: number; pageSize: number }> = {}) {
    const filters = { query, accountType, direction, platform, ...overrides };
    const size = overrides.pageSize ?? pageSize;
    let targetPage = Math.max(1, overrides.page ?? page);
    setFetching(true);
    try {
      const request = (pageNumber: number) => jsonRequest({ page: pageNumber, page_size: size, query: filters.query, account_type: filters.accountType || null, content_direction: filters.direction || null, platform: filters.platform || null });
      let result = await readJson<AccountSearchResult>("/api/v8/accounts/search", request(targetPage));
      const lastPage = Math.max(1, Math.ceil(result.total / size));
      if (targetPage > lastPage) {
        targetPage = lastPage;
        result = await readJson<AccountSearchResult>("/api/v8/accounts/search", request(targetPage));
      }
      setItems(result.items); setTotal(result.total); setUnassociated(result.legacy_unassociated_content_count);
      setPendingCount(result.pending_platform_identity_count); setPending(result.pending_platform_identities);
      setPage(targetPage); setPageSize(size);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号读取失败"); }
    finally { setLoading(false); setFetching(false); }
  }
  useEffect(() => {
    readJson<AccountSearchResult>("/api/v8/accounts/search", jsonRequest({ page: 1, page_size: 50 }))
      .then((result) => { setItems(result.items); setTotal(result.total); setUnassociated(result.legacy_unassociated_content_count); setPendingCount(result.pending_platform_identity_count); setPending(result.pending_platform_identities); })
      .catch((reason) => setError(reason instanceof Error ? reason.message : "账号读取失败"))
      .finally(() => setLoading(false));
  }, []);

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
      const wasEdit = Boolean(form.id); setForm(null); await reload(); setMessage(wasEdit ? "账号已更新" : "账号已新增");
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号保存失败"); }
    finally { setSaving(false); }
  }

  async function importCsv(file: File) {
    setSaving(true); setError(""); setMessage("");
    try {
      const rows = parseCsv(await file.text());
      if (!rows.length) throw new Error("CSV 中没有可导入的数据行");
      const result = await readJson<{ inserted_rows: number; updated_rows: number; rejected_rows: number }>("/api/v8/accounts/import", jsonRequest({ source_name: file.name, rows }));
      await reload(); setMessage(`导入完成：新增 ${result.inserted_rows}，覆盖 ${result.updated_rows}，拒绝 ${result.rejected_rows}`);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号导入失败"); }
    finally { setSaving(false); }
  }

  return <AppShell active="accounts">
    <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {loading ? <Loading label="正在读取账号库" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">手机号为业务主键</span><h2>账号主数据</h2><p>一个手机号对应一行账号主数据；平台粉丝量未采集时显示“—”。</p></div><div className="placeholder-actions"><button className="primary small" onClick={() => edit()}>新增账号</button><a className="secondary button-link" href={`${API_BASE}/api/v8/accounts/export`}>下载 CSV</a><label className="secondary button-link">批量导入<input className="file-input" type="file" accept=".csv,text/csv" disabled={saving} onChange={(event) => { const file = event.target.files?.[0]; if (file) void importCsv(file); event.currentTarget.value = ""; }} /></label></div></div>
      <div className="filter-bar"><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="手机号、运营人员、UID、昵称" onKeyDown={(event) => { if (event.key === "Enter") void reload({ page: 1 }); }} /><select value={accountType} onChange={(event) => { setAccountType(event.target.value); void reload({ accountType: event.target.value, page: 1 }); }}><option value="">全部账号类型</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option><option value="unknown">未知</option></select><select value={direction} onChange={(event) => { setDirection(event.target.value); void reload({ direction: event.target.value, page: 1 }); }}><option value="">全部内容方向</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option><option value="unknown">未知</option></select><select value={platform} onChange={(event) => { setPlatform(event.target.value); void reload({ platform: event.target.value, page: 1 }); }}><option value="">全部平台</option>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select><button className="secondary" onClick={() => void reload({ page: 1 })}>搜索</button><span>{total} 个账号</span></div>
      <article className="panel table-panel account-master-panel">
        <Pagination page={page} pageSize={pageSize} total={total} busy={fetching || saving} ariaLabel="账号分页" unitLabel="个账号" placement="top" onChange={(next) => void reload(next)} />
        <div className="table-scroll"><table className="account-master-table">
        <caption className="visually-hidden">以手机号为锚点的账号主数据列表</caption>
        <colgroup>
          <col className="account-phone-col" /><col className="account-operator-col" /><col className="account-type-col" /><col className="account-direction-col" />
        </colgroup>
        {platformKeys.map((key) => <colgroup key={key} data-platform-columns={key}>
          <col className="account-uid-col" /><col className="account-realname-col" /><col className="account-nickname-col" /><col className="account-followers-col" /><col className="account-content-count-col" />
        </colgroup>)}
        <colgroup>
          <col className="account-status-col" /><col className="account-action-col" />
        </colgroup>
        <thead>
          <tr className="account-group-row">
            <th className="account-base-group" colSpan={4} scope="colgroup">账号基础信息</th>
            {platformKeys.map((key) => <th className="account-platform-group" data-platform={key} colSpan={5} scope="colgroup" key={key}><span className="account-platform-heading"><PlatformHeaderMark platformKey={key} />{label(key)}</span></th>)}
            <th className="account-management-group" colSpan={2} scope="colgroup">账号管理</th>
          </tr>
          <tr className="account-column-row"><th className="account-sticky account-phone" scope="col">手机号</th><th className="account-sticky account-operator" scope="col">运营人员</th><th className="account-sticky account-type" scope="col">账号类型</th><th className="account-sticky account-direction" scope="col">内容方向</th>{platformKeys.map((key) => <Fragment key={key}><th className="account-platform-start" scope="col">UID</th><th scope="col">是否实名</th><th scope="col">昵称</th><th className="account-number-cell" scope="col">粉丝量</th><th className="account-number-cell" scope="col">关联内容量</th></Fragment>)}<th scope="col">状态</th><th scope="col">操作</th></tr>
        </thead>
        <tbody>
          {items.map((item) => <tr key={item.id}><th className="account-sticky account-phone" scope="row"><strong>{item.phone}</strong></th><td className="account-sticky account-operator">{item.operator_name || "未填写"}</td><td className="account-sticky account-type">{label(item.account_type)}</td><td className="account-sticky account-direction">{label(item.content_direction)}</td><AccountPlatformCells account={item} /><td>{item.enabled ? "运营中" : "停用"}</td><td><button className="text-button" onClick={() => edit(item)}>修改</button></td></tr>)}
          {!items.length && <tr><td className="account-master-empty" colSpan={26}>暂无账号，请新增账号或批量导入</td></tr>}
        </tbody>
      </table></div></article>
      <article className="panel pending-identity-panel">
        <div className="panel-head"><div><span className="eyebrow">待确认平台身份</span><h3>待归属平台身份 · {pendingCount}</h3><p>{unassociated} 条存量内容尚未关联手机号账号；账号新增或导入匹配 UID 后会自动回填。</p></div></div>
        <div className="pending-identity-list table-scroll">
          <table className="pending-identity-table">
            <caption className="visually-hidden">待归属平台身份列表</caption>
            <thead><tr><th>平台</th><th>平台 UID</th><th>昵称</th><th>关联内容</th></tr></thead>
            <tbody>
              {pending.map((item) => <tr key={`${item.platform}:${item.uid}`}><td><span className="pending-platform-label">{label(item.platform)}</span></td><td><strong>{item.uid}</strong></td><td>{item.nickname || "昵称缺失"}</td><td>{item.content_count} 条</td></tr>)}
              {!pending.length && <tr><td className="pending-identity-empty" colSpan={4}>暂无待归属平台身份</td></tr>}
            </tbody>
          </table>
        </div>
      </article>
    </section>}
    {form && <div className="modal-backdrop" role="presentation"><section className="modal-panel operation-modal" role="dialog" aria-modal="true" aria-label="编辑账号"><div className="panel-head"><div><span className="eyebrow">账号主数据</span><h3>{form.id ? "修改账号" : "新增账号"}</h3></div><button className="modal-close" onClick={() => setForm(null)} aria-label="关闭">×</button></div><div className="modal-fields"><label>手机号<input value={form.phone} onChange={(event) => setForm({ ...form, phone: event.target.value })} /></label><label>运营人员<input value={form.operatorName} onChange={(event) => setForm({ ...form, operatorName: event.target.value })} /></label><label>账号类型<select value={form.accountType} onChange={(event) => setForm({ ...form, accountType: event.target.value })}><option value="unknown">未知</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option></select></label><label>内容方向<select value={form.contentDirection} onChange={(event) => setForm({ ...form, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label><label className="toggle-field"><input type="checkbox" checked={form.enabled} onChange={(event) => setForm({ ...form, enabled: event.target.checked })} />运营中</label></div><div className="platform-editor">{platformKeys.map((key) => <fieldset key={key}><legend>{label(key)}</legend><label>UID<input value={form.platforms[key].uid} onChange={(event) => setForm({ ...form, platforms: { ...form.platforms, [key]: { ...form.platforms[key], uid: event.target.value } } })} /></label><label>实名<select value={form.platforms[key].realNameStatus} onChange={(event) => setForm({ ...form, platforms: { ...form.platforms, [key]: { ...form.platforms[key], realNameStatus: event.target.value } } })}><option value="unknown">未知</option><option value="yes">是</option><option value="no">否</option></select></label><label>昵称<input value={form.platforms[key].nickname} onChange={(event) => setForm({ ...form, platforms: { ...form.platforms, [key]: { ...form.platforms[key], nickname: event.target.value } } })} /></label></fieldset>)}</div><div className="modal-actions"><button className="secondary" onClick={() => setForm(null)}>取消</button><button className="primary" disabled={saving} onClick={() => void save()}>{saving ? "保存中" : "保存账号"}</button></div></section></div>}
  </AppShell>;
}
