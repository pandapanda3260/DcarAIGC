"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import AppShell from "../components/AppShell";
import { Feedback, Loading } from "../components/Feedback";
import { API_BASE, jsonRequest, parseCsv, readJson } from "../lib/api";
import { formatDateTime, label, platformKeys } from "../lib/format";
import type { ContentItem, SellingPoint, SellingPointResponse } from "../lib/types";
import EvidenceModal from "./EvidenceModal";
import { buildContentSaveOperation, emptyContentForm, toShanghaiDateTimeLocal, type ContentForm } from "./contentForm";

const PAGE_SIZE_OPTIONS = [20, 50, 100];

function pageWindow(current: number, totalPages: number): Array<number | "left-gap" | "right-gap"> {
  if (totalPages <= 7) return Array.from({ length: totalPages }, (_, index) => index + 1);
  const middle = [current - 1, current, current + 1].filter((value, index, list) => list.indexOf(value) === index && value > 1 && value < totalPages);
  const slots: Array<number | "left-gap" | "right-gap"> = [1];
  if (middle.length && middle[0] > 2) slots.push("left-gap");
  slots.push(...middle);
  if (middle.length && middle[middle.length - 1] < totalPages - 1) slots.push("right-gap");
  slots.push(totalPages);
  return slots;
}

export default function ContentsPage() {
  const [items, setItems] = useState<ContentItem[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [jumpValue, setJumpValue] = useState("");
  const [fetching, setFetching] = useState(false);
  const [query, setQuery] = useState("");
  const [platform, setPlatform] = useState("");
  const [accountType, setAccountType] = useState("");
  const [direction, setDirection] = useState("");
  const [reviewStatus, setReviewStatus] = useState("");
  const [sellingPoint, setSellingPoint] = useState("");
  const [sellingPoints, setSellingPoints] = useState<SellingPoint[]>([]);
  const [form, setForm] = useState<ContentForm | null>(null);
  const [originalForm, setOriginalForm] = useState<ContentForm | null>(null);
  const [evidenceItem, setEvidenceItem] = useState<{ item: ContentItem; review: boolean } | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const feedback = useCallback((nextError: string, nextMessage = "") => { setError(nextError); setMessage(nextMessage); }, []);
  const reload = useCallback(async (overrides: Partial<{ query: string; platform: string; accountType: string; direction: string; reviewStatus: string; sellingPoint: string; page: number; pageSize: number }> = {}) => {
    const filters = { query, platform, accountType, direction, reviewStatus, sellingPoint, ...overrides };
    const size = overrides.pageSize ?? pageSize;
    let targetPage = Math.max(1, overrides.page ?? page);
    setFetching(true);
    try {
      const request = (pageNumber: number) => jsonRequest({ page: pageNumber, page_size: size, query: filters.query, platform: filters.platform || null, account_type: filters.accountType || null, content_direction: filters.direction || null, review_status: filters.reviewStatus || null, selling_point: filters.sellingPoint || null });
      let result = await readJson<{ items: ContentItem[]; total: number }>("/api/v8/contents/search", request(targetPage));
      const lastPage = Math.max(1, Math.ceil(result.total / size));
      if (targetPage > lastPage) {
        targetPage = lastPage;
        result = await readJson<{ items: ContentItem[]; total: number }>("/api/v8/contents/search", request(targetPage));
      }
      setItems(result.items); setTotal(result.total); setPage(targetPage); setPageSize(size); setJumpValue("");
    } catch (reason) { setError(reason instanceof Error ? reason.message : "内容读取失败"); }
    finally { setLoading(false); setFetching(false); }
  }, [accountType, direction, platform, query, reviewStatus, sellingPoint, page, pageSize]);
  useEffect(() => {
    readJson<{ items: ContentItem[]; total: number }>("/api/v8/contents/search", jsonRequest({ page: 1, page_size: 50 }))
      .then((result) => { setItems(result.items); setTotal(result.total); })
      .catch((reason) => setError(reason instanceof Error ? reason.message : "内容读取失败"))
      .finally(() => setLoading(false));
    readJson<SellingPointResponse>("/api/v8/selling-points")
      .then((result) => setSellingPoints(result.items))
      .catch(() => setSellingPoints([]));
  }, []);
  const sellingPointSelectRef = useRef<HTMLSelectElement | null>(null);
  const sellingPointLabel = sellingPoint === "" ? "全部卖点" : sellingPoint === "__none__" ? "证据缺失" : (() => { const point = sellingPoints.find((item) => item.code === sellingPoint); return point ? `${point.code} · ${point.label}` : sellingPoint; })();
  useEffect(() => {
    const node = sellingPointSelectRef.current;
    if (!node) return;
    const context = document.createElement("canvas").getContext("2d");
    if (!context) return;
    const style = window.getComputedStyle(node as unknown as Element);
    context.font = `${style.fontStyle} ${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
    node.style.width = `${Math.ceil(context.measureText(sellingPointLabel).width) + 46}px`;
  }, [sellingPointLabel, loading]);
  const sellingPointLabelByCode = new Map<string, string>(sellingPoints.map((point) => [point.code, point.label]));

  function edit(item?: ContentItem) {
    const next = item ? { id: item.id, platform: item.platform, platformContentId: item.platform_content_id ?? "", canonicalUrl: item.canonical_url, publishedAt: toShanghaiDateTimeLocal(item.published_at), title: item.title, body: item.body ?? "", contentType: item.content_type ?? "unknown", accountUid: item.raw_account_uid ?? "", accountName: item.raw_account_name ?? "", accountType: item.account_type, contentDirection: item.content_direction } : { ...emptyContentForm };
    setForm(next);
    setOriginalForm(item ? { ...next } : null);
  }
  async function save() {
    if (!form) return;
    setSaving(true); feedback("");
    try {
      const operation = buildContentSaveOperation(form, originalForm);
      if (operation.unchanged) { setForm(null); setOriginalForm(null); feedback("", "内容未发生修改"); return; }
      await readJson(operation.path, jsonRequest(operation.body, operation.method));
      const wasEdit = Boolean(form.id); setForm(null); setOriginalForm(null); await reload(); feedback("", wasEdit ? "内容已更新" : "内容已新增");
    } catch (reason) { feedback(reason instanceof Error ? reason.message : "内容保存失败"); }
    finally { setSaving(false); }
  }
  async function importCsv(file: File) {
    setSaving(true); feedback("");
    try {
      const rows = parseCsv(await file.text()); if (!rows.length) throw new Error("CSV 中没有可导入的数据行");
      const request = { source_name: file.name, rows };
      const validation = await readJson<{ total: number; rejected: number }>("/api/v8/contents/validate", jsonRequest(request));
      if (validation.rejected) throw new Error(`内容导入校验失败：${validation.rejected} / ${validation.total} 行无效，未写入数据库`);
      const result = await readJson<{ inserted_rows: number; updated_rows: number; rejected_rows: number }>("/api/v8/contents/import", jsonRequest(request));
      await reload(); feedback("", `导入完成：新增 ${result.inserted_rows}，覆盖 ${result.updated_rows}，拒绝 ${result.rejected_rows}`);
    } catch (reason) { feedback(reason instanceof Error ? reason.message : "内容导入失败"); }
    finally { setSaving(false); }
  }
  async function updateData(item: ContentItem) {
    setSaving(true); feedback("");
    try { const result = await readJson<{ status: string; provider_cost: number }>(`/api/v8/contents/${item.id}/update-data`, { method: "POST" }); await reload(); feedback("", `${item.link_id} 更新${result.status === "succeeded" ? "完成" : "部分完成"}，供应商费用 $${Number(result.provider_cost).toFixed(3)}`); }
    catch (reason) { feedback(reason instanceof Error ? reason.message : "数据更新失败"); }
    finally { setSaving(false); }
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const paginationBusy = fetching || saving;
  function goToPage(next: number) {
    const target = Math.min(Math.max(1, next), totalPages);
    if (target === page || fetching) return;
    void reload({ page: target });
  }
  function jumpToPage() {
    const parsed = Number.parseInt(jumpValue, 10);
    if (Number.isNaN(parsed)) { setJumpValue(""); return; }
    goToPage(parsed);
  }

  return <AppShell active="contents">
    <Feedback error={error} message={message} onClose={() => feedback("")} />
    {loading ? <Loading label="正在读取内容库" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">CONTENT EVIDENCE LIBRARY</span><h2>发布内容明细</h2><p>更新数据同步刷新详情、指标和本地媒体证据；重复提醒指向最早发布内容。</p></div><div className="placeholder-actions"><button className="primary button-link" onClick={() => edit()}>新增内容</button><a className="secondary button-link" href={`${API_BASE}/api/v8/contents/export`}>下载 CSV</a><label className="secondary button-link">批量导入<input className="file-input" type="file" accept=".csv,text/csv" disabled={saving} onChange={(event) => { const file = event.target.files?.[0]; if (file) void importCsv(file); event.currentTarget.value = ""; }} /></label></div></div>
      <div className="filter-bar"><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="链接 ID、标题、UID、昵称或链接" onKeyDown={(event) => { if (event.key === "Enter") void reload({ page: 1 }); }} /><select value={platform} onChange={(event) => { setPlatform(event.target.value); void reload({ platform: event.target.value, page: 1 }); }}><option value="">全部平台</option>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select><select value={accountType} onChange={(event) => { setAccountType(event.target.value); void reload({ accountType: event.target.value, page: 1 }); }}><option value="">全部账号类型</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option><option value="unknown">未知</option></select><select value={direction} onChange={(event) => { setDirection(event.target.value); void reload({ direction: event.target.value, page: 1 }); }}><option value="">全部内容方向</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option><option value="unknown">未知</option></select><select className="selling-point-filter" ref={sellingPointSelectRef} value={sellingPoint} onChange={(event) => { setSellingPoint(event.target.value); void reload({ sellingPoint: event.target.value, page: 1 }); }}><option value="">全部卖点</option><option value="__none__">证据缺失</option>{sellingPoints.map((point) => <option key={point.code} value={point.code} title={point.label}>{point.code} · {point.label}</option>)}</select><select value={reviewStatus} onChange={(event) => { setReviewStatus(event.target.value); void reload({ reviewStatus: event.target.value, page: 1 }); }}><option value="">全部复核状态</option><option value="pending">待复核</option><option value="terminal_failed">终态不可用</option><option value="resolved">已处理</option></select><button className="secondary" onClick={() => void reload({ page: 1 })}>搜索</button><span>{total} 条内容</span></div>
      <article className="panel table-panel"><div className="table-scroll"><table className="content-table"><thead><tr><th>内容ID / 平台ID / 标题</th><th>平台 / 发布时间</th><th>账号</th><th>阅读 / 评论</th><th>账号类型</th><th>内容方向</th><th>卖点</th><th>垂直度</th><th>预估拉新</th><th>预估拉活</th><th>预估线索</th><th>重复提醒</th><th>操作</th></tr></thead><tbody>{items.map((item) => <tr key={item.id}><td><a href={item.canonical_url} target="_blank" rel="noreferrer">{item.link_id} · {item.platform_content_id || "平台内容 ID 缺失"}</a><span>{item.title || "标题缺失"}</span></td><td>{label(item.platform)}<span className="cell-subline">{formatDateTime(item.published_at)}</span></td><td>{item.raw_account_name || "昵称缺失"}<span className="cell-subline">{item.raw_account_uid || "UID 缺失"}</span></td><td>{item.view_count ?? "暂不可计算"}<span>评论 {item.comment_count ?? "暂不可计算"}</span></td><td>{label(item.account_type)}</td><td>{label(item.content_direction)}</td><td>{item.primary_selling_point_code ? <><span className="selling-point-name" title={`${item.primary_selling_point_code} · ${sellingPointLabelByCode.get(item.primary_selling_point_code) ?? "不在当前卖点库"}`}>{sellingPointLabelByCode.get(item.primary_selling_point_code) ?? item.primary_selling_point_code}</span><span className="cell-subline">{item.evidence_level || "证据缺失"}</span></> : item.evidence_level ? <>暂不可计算<span className="cell-subline">{item.evidence_level}</span></> : "证据缺失"}{Boolean(item.evaluation_is_stale) && <span className="status-badge stale-evaluation">旧规则评估</span>}</td><td>{item.content_automotive_score == null ? "暂不可计算" : `${item.content_automotive_score}%`}</td><td>暂不可计算</td><td>暂不可计算</td><td>暂不可计算</td><td>{item.duplicate_original_link_id || "—"}</td><td><span className="row-actions"><button className="text-button" disabled={saving} onClick={() => setEvidenceItem({ item, review: false })}>查看证据</button><button className="text-button" disabled={saving} onClick={() => void updateData(item)}>更新数据</button><button className="text-button" disabled={saving} onClick={() => edit(item)}>修改</button>{item.terminal_review_count > 0 ? <span className="status-badge terminal">终态</span> : item.pending_review_count > 0 ? <button className="text-button review-entry" disabled={saving} onClick={() => setEvidenceItem({ item, review: true })}>待复核</button> : item.review_status === "resolved" && item.review_queue_id ? <button className="text-button review-entry" disabled={saving} onClick={() => setEvidenceItem({ item, review: true })}>再次复核</button> : null}</span></td></tr>)}</tbody></table></div>
      <footer className="pagination-bar">
        <span className="pagination-info">共 {total} 条 · 第 {page} / {totalPages} 页</span>
        <nav className="pagination-controls" aria-label="内容分页">
          <button className="page-button" disabled={paginationBusy || page <= 1} onClick={() => goToPage(1)}>首页</button>
          <button className="page-button" disabled={paginationBusy || page <= 1} onClick={() => goToPage(page - 1)}>上一页</button>
          {pageWindow(page, totalPages).map((slot) => typeof slot === "number"
            ? <button key={slot} className={`page-button${slot === page ? " active" : ""}`} disabled={paginationBusy} aria-current={slot === page ? "page" : undefined} onClick={() => goToPage(slot)}>{slot}</button>
            : <span key={slot} className="page-ellipsis">…</span>)}
          <button className="page-button" disabled={paginationBusy || page >= totalPages} onClick={() => goToPage(page + 1)}>下一页</button>
          <button className="page-button" disabled={paginationBusy || page >= totalPages} onClick={() => goToPage(totalPages)}>末页</button>
        </nav>
        <div className="pagination-jump">
          <label>每页<select value={pageSize} disabled={paginationBusy} onChange={(event) => void reload({ pageSize: Number(event.target.value), page: 1 })}>{PAGE_SIZE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}</select>条</label>
          <label>跳至<input value={jumpValue} disabled={paginationBusy} inputMode="numeric" placeholder="页码" onChange={(event) => setJumpValue(event.target.value.replace(/[^0-9]/g, ""))} onKeyDown={(event) => { if (event.key === "Enter") jumpToPage(); }} />页</label>
          <button className="page-button" disabled={paginationBusy || !jumpValue} onClick={jumpToPage}>跳转</button>
        </div>
      </footer></article>
    </section>}
    {form && <div className="modal-backdrop" role="presentation"><section className="review-modal operation-modal" role="dialog" aria-modal="true" aria-label="编辑内容"><div className="panel-head"><div><span className="eyebrow">CONTENT MASTER DATA</span><h3>{form.id ? "修改内容" : "新增内容"}</h3></div><button className="modal-close" onClick={() => setForm(null)} aria-label="关闭">×</button></div><div className="review-fields"><label>发布平台<select value={form.platform} onChange={(event) => setForm({ ...form, platform: event.target.value })}>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select></label><label>内容类型<select value={form.contentType} onChange={(event) => setForm({ ...form, contentType: event.target.value })}><option value="video">视频</option><option value="image">图文</option><option value="unknown">未知</option></select></label><label className="span-two">链接<input value={form.canonicalUrl} onChange={(event) => setForm({ ...form, canonicalUrl: event.target.value })} /></label><label>平台内容 ID<input value={form.platformContentId} onChange={(event) => setForm({ ...form, platformContentId: event.target.value })} /></label><label>发布日期<input type="datetime-local" value={form.publishedAt} onChange={(event) => setForm({ ...form, publishedAt: event.target.value })} /></label><label className="span-two">标题<input value={form.title} onChange={(event) => setForm({ ...form, title: event.target.value })} /></label><label className="span-two">正文<textarea value={form.body} onChange={(event) => setForm({ ...form, body: event.target.value })} /></label><label>账号 UID<input value={form.accountUid} onChange={(event) => setForm({ ...form, accountUid: event.target.value })} /></label><label>账号昵称<input value={form.accountName} onChange={(event) => setForm({ ...form, accountName: event.target.value })} /></label><label>账号类型<select value={form.accountType} onChange={(event) => setForm({ ...form, accountType: event.target.value })}><option value="unknown">未知</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option></select></label><label>内容方向<select value={form.contentDirection} onChange={(event) => setForm({ ...form, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label></div><div className="modal-actions"><button className="secondary" onClick={() => setForm(null)}>取消</button><button className="primary" disabled={saving} onClick={() => void save()}>{saving ? "保存中" : "保存内容"}</button></div></section></div>}
    {evidenceItem && <EvidenceModal item={evidenceItem.item} beginReview={evidenceItem.review} onClose={() => setEvidenceItem(null)} onChanged={() => reload()} onFeedback={feedback} />}
  </AppShell>;
}
