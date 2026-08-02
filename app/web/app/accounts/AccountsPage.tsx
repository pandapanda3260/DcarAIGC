"use client";

import { useEffect, useState } from "react";
import AppShell from "../components/AppShell";
import { Feedback, Loading } from "../components/Feedback";
import { API_BASE, jsonRequest, parseCsv, readJson } from "../lib/api";
import { label, platformKeys } from "../lib/format";
import type { Account, PendingPlatformIdentity } from "../lib/types";

type AccountForm = {
  id: number | null; phone: string; operatorName: string; accountType: string;
  contentDirection: string; enabled: boolean;
  platforms: Record<string, { uid: string; nickname: string; realNameStatus: string }>;
};
const blankIdentity = () => ({ uid: "", nickname: "", realNameStatus: "unknown" });
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
  const [query, setQuery] = useState("");
  const [accountType, setAccountType] = useState("");
  const [direction, setDirection] = useState("");
  const [platform, setPlatform] = useState("");
  const [form, setForm] = useState<AccountForm | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  async function reload(overrides: Partial<{ query: string; accountType: string; direction: string; platform: string }> = {}) {
    const filters = { query, accountType, direction, platform, ...overrides };
    try {
      const result = await readJson<{ items: Account[]; total: number; legacy_unassociated_content_count: number; pending_platform_identity_count: number; pending_platform_identities: PendingPlatformIdentity[] }>("/api/v8/accounts/search", jsonRequest({ page: 1, page_size: 50, query: filters.query, account_type: filters.accountType || null, content_direction: filters.direction || null, platform: filters.platform || null }));
      setItems(result.items); setTotal(result.total); setUnassociated(result.legacy_unassociated_content_count);
      setPendingCount(result.pending_platform_identity_count); setPending(result.pending_platform_identities);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "账号读取失败"); }
    finally { setLoading(false); }
  }
  useEffect(() => {
    readJson<{ items: Account[]; total: number; legacy_unassociated_content_count: number; pending_platform_identity_count: number; pending_platform_identities: PendingPlatformIdentity[] }>("/api/v8/accounts/search", jsonRequest({ page: 1, page_size: 50 }))
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

  return <AppShell active="accounts" actions={<button className="primary small" onClick={() => edit()}>新增账号</button>}>
    <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {loading ? <Loading label="正在读取账号库" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">PHONE AS BUSINESS KEY</span><h2>账号主数据</h2><p>手机号完整显示且作为唯一键；导入重复手机号时由新数据覆盖旧数据。</p></div><div className="placeholder-actions"><a className="secondary button-link" href={`${API_BASE}/api/v8/accounts/export`}>下载 CSV</a><label className="secondary button-link">批量导入<input className="file-input" type="file" accept=".csv,text/csv" disabled={saving} onChange={(event) => { const file = event.target.files?.[0]; if (file) void importCsv(file); event.currentTarget.value = ""; }} /></label></div></div>
      <div className="filter-bar"><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="手机号、运营人员、UID、昵称" onKeyDown={(event) => { if (event.key === "Enter") void reload(); }} /><select value={accountType} onChange={(event) => { setAccountType(event.target.value); void reload({ accountType: event.target.value }); }}><option value="">全部账号类型</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option><option value="unknown">未知</option></select><select value={direction} onChange={(event) => { setDirection(event.target.value); void reload({ direction: event.target.value }); }}><option value="">全部内容方向</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option><option value="unknown">未知</option></select><select value={platform} onChange={(event) => { setPlatform(event.target.value); void reload({ platform: event.target.value }); }}><option value="">全部平台</option>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select><button className="secondary" onClick={() => void reload()}>搜索</button><span>{total} 个账号</span></div>
      <article className="panel table-panel"><div className="table-scroll"><table><thead><tr><th>手机号 / 运营人员</th><th>账号类型</th><th>内容方向</th><th>平台身份</th><th>状态</th><th>操作</th></tr></thead><tbody>{items.map((item) => <tr key={item.id}><td><strong>{item.phone}</strong><span>{item.operator_name || "未填写运营人员"}</span></td><td>{label(item.account_type)}</td><td>{label(item.content_direction)}</td><td>{item.platforms.length ? item.platforms.map((identity) => <span className="identity-line" key={`${identity.platform}:${identity.uid}`}>{label(identity.platform)} · {identity.uid} · {identity.nickname || "无昵称"} · 实名 {label(identity.real_name_status)}</span>) : "未绑定平台身份"}</td><td>{item.enabled ? "运营中" : "停用"}</td><td><button className="text-button" onClick={() => edit(item)}>修改</button></td></tr>)}</tbody></table></div></article>
      <article className="panel"><div className="panel-head"><div><span className="eyebrow">PENDING PLATFORM IDENTITIES</span><h3>待归属平台身份 · {pendingCount}</h3><p>{unassociated} 条存量内容尚未关联手机号账号；账号新增或导入匹配 UID 后会自动回填。</p></div></div><div className="pending-identity-grid">{pending.map((item) => <div key={`${item.platform}:${item.uid}`}><strong>{label(item.platform)} · {item.uid}</strong><span>{item.nickname || "昵称缺失"} · {item.content_count} 条内容</span></div>)}</div></article>
    </section>}
    {form && <div className="modal-backdrop" role="presentation"><section className="review-modal operation-modal" role="dialog" aria-modal="true" aria-label="编辑账号"><div className="panel-head"><div><span className="eyebrow">ACCOUNT MASTER DATA</span><h3>{form.id ? "修改账号" : "新增账号"}</h3></div><button className="modal-close" onClick={() => setForm(null)} aria-label="关闭">×</button></div><div className="review-fields"><label>手机号<input value={form.phone} onChange={(event) => setForm({ ...form, phone: event.target.value })} /></label><label>运营人员<input value={form.operatorName} onChange={(event) => setForm({ ...form, operatorName: event.target.value })} /></label><label>账号类型<select value={form.accountType} onChange={(event) => setForm({ ...form, accountType: event.target.value })}><option value="unknown">未知</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option></select></label><label>内容方向<select value={form.contentDirection} onChange={(event) => setForm({ ...form, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label><label className="toggle-field"><input type="checkbox" checked={form.enabled} onChange={(event) => setForm({ ...form, enabled: event.target.checked })} />运营中</label></div><div className="platform-editor">{platformKeys.map((key) => <fieldset key={key}><legend>{label(key)}</legend><label>UID<input value={form.platforms[key].uid} onChange={(event) => setForm({ ...form, platforms: { ...form.platforms, [key]: { ...form.platforms[key], uid: event.target.value } } })} /></label><label>实名<select value={form.platforms[key].realNameStatus} onChange={(event) => setForm({ ...form, platforms: { ...form.platforms, [key]: { ...form.platforms[key], realNameStatus: event.target.value } } })}><option value="unknown">未知</option><option value="yes">是</option><option value="no">否</option></select></label><label>昵称<input value={form.platforms[key].nickname} onChange={(event) => setForm({ ...form, platforms: { ...form.platforms, [key]: { ...form.platforms[key], nickname: event.target.value } } })} /></label></fieldset>)}</div><div className="modal-actions"><button className="secondary" onClick={() => setForm(null)}>取消</button><button className="primary" disabled={saving} onClick={() => void save()}>{saving ? "保存中" : "保存账号"}</button></div></section></div>}
  </AppShell>;
}
