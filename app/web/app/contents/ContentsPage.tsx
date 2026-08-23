"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppShell from "../components/AppShell";
import { Feedback, Loading, Notice } from "../components/Feedback";
import { Pagination } from "../components/Pagination";
import { API_BASE, jsonRequest, parseCsv, readJson } from "../lib/api";
import { formatDateTime, label, platformKeys } from "../lib/format";
import { buildContentSearchRequest, lastPageFor } from "../lib/queryContracts";
import { activeSellingPointsQueryOptions, contentSearchQueryOptions, queryKeys, spuAssetsQueryOptions } from "../lib/queries";
import type { ContentItem, ContentTagSpu } from "../lib/types";
import EvidenceModal from "./EvidenceModal";
import { buildContentSaveOperation, emptyContentForm, toShanghaiDateTimeLocal, type ContentForm } from "./contentForm";

const EVIDENCE_LEVEL_HINTS: Record<string, string> = {
  V3: "V3：信息完整，视频、语音和画面文字均可用，可以自动评估",
  V2: "V2：信息较完整，至少一种媒体资料能说明主要内容，可以自动评估",
  V1: "V1：信息不足，只有标题、正文或话题，暂时无法得出评估结果",
  V0: "V0：无法评估，没有可用的正文、语音或画面文字",
};

const EVIDENCE_LEVEL_LABELS: Record<string, string> = {
  V3: "信息完整（V3）",
  V2: "有媒体资料（V2）",
  V1: "只有文字（V1）",
  V0: "资料不可用（V0）",
};

// 车型列两行制：第一行 品牌+型号（车系名已含品牌时不重复），第二行 相关标签重点内容
// 标题默认折叠两行，溢出时在第二行末尾内联一个无边框「展开」。
// 是否溢出必须实测（scrollHeight > clientHeight），不能按字数猜——列宽随窗口变化，
// 所以用 ResizeObserver 跟随重算；展开后按钮改为跟在全文末尾。
function ContentTitle({ text }: { text: string }) {
  const [expanded, setExpanded] = useState(false);
  const [overflowing, setOverflowing] = useState(false);
  const textRef = useRef<HTMLSpanElement | null>(null);

  useEffect(() => {
    const node = textRef.current;
    if (!node) return;
    const measure = () => setOverflowing(node.scrollHeight > node.clientHeight + 1);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(node);
    return () => observer.disconnect();
    // 依赖里带上 expanded：展开态是 display:inline，而 ResizeObserver 规范不观测
    // 非替换的 inline 元素，只靠它回调会漏掉收起后的那次重新测量。
  }, [text, expanded]);

  return (
    <span className="content-title" data-expanded={expanded ? "true" : undefined}>
      <span className="content-title-text" ref={textRef}>{text}</span>
      {(overflowing || expanded) && (
        <button type="button" className="content-title-toggle" aria-expanded={expanded} onClick={() => setExpanded((value) => !value)}>
          {expanded ? "收起" : "展开"}
        </button>
      )}
    </span>
  );
}

function spuDisplayName(spu: ContentTagSpu) {
  return spu.series.startsWith(spu.brand) ? spu.series : `${spu.brand} ${spu.series}`;
}

function spuSubline(item: ContentItem) {
  const spu = item.spu;
  if (!spu) return "";
  const parts = [spu.resolved_level === "trim" && spu.trim_label ? spu.trim_label : "未细化"];
  const alias = spu.matched_aliases[0] ?? "";
  if (alias && !spu.series.toLowerCase().includes(alias.toLowerCase()) && !alias.toLowerCase().includes(spu.series.toLowerCase())) {
    parts.push(`命中「${alias}」`);
  }
  if (item.spu_secondary_count > 0) parts.push(`另提及 ${item.spu_secondary_count} 车系`);
  return parts.join(" · ");
}

function spuCellTitle(spu: ContentTagSpu) {
  const name = `${spu.brand} ${spu.series}${spu.trim_label ? ` · ${spu.trim_label}` : "（未细化）"}`;
  const aliases = spu.matched_aliases.length > 0 ? `｜命中：${spu.matched_aliases.join("、")}` : "";
  return `${name}｜识别评分 ${spu.score}${aliases}`;
}

export default function ContentsPage() {
  const [query, setQuery] = useState("");
  const [platform, setPlatform] = useState("");
  const [accountType, setAccountType] = useState("");
  const [direction, setDirection] = useState("");
  const [sellingPoint, setSellingPoint] = useState("");
  const [spuSeries, setSpuSeries] = useState("");
  const [audience, setAudience] = useState("");
  const [scene, setScene] = useState("");
  const [appliedRequest, setAppliedRequest] = useState(() => buildContentSearchRequest({
    query: "", platform: "", accountType: "", direction: "", sellingPoint: "",
    spuSeries: "", audience: "", scene: "",
  }, 1, 50));
  const [form, setForm] = useState<ContentForm | null>(null);
  const [originalForm, setOriginalForm] = useState<ContentForm | null>(null);
  const [evidenceItem, setEvidenceItem] = useState<ContentItem | null>(null);
  const [saving, setSaving] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const queryClient = useQueryClient();
  const contentsQuery = useQuery(contentSearchQueryOptions(appliedRequest));
  const sellingPointsQuery = useQuery(activeSellingPointsQueryOptions());
  const spuAssetsQuery = useQuery(spuAssetsQueryOptions());
  const items = contentsQuery.data?.items ?? [];
  const total = contentsQuery.data?.total ?? 0;
  const contentsReadFailed = contentsQuery.isLoadingError || retrying;
  const sellingPoints = sellingPointsQuery.data?.items ?? [];
  const spuAssets = spuAssetsQuery.data?.ready ? spuAssetsQuery.data : null;

  const feedback = useCallback((nextError: string, nextMessage = "") => { setError(nextError); setMessage(nextMessage); }, []);
  function retryContentsRead() {
    if (retrying) return;
    setRetrying(true);
    void contentsQuery.refetch().finally(() => setRetrying(false));
  }
  function applySearch(overrides: Partial<{ query: string; platform: string; accountType: string; direction: string; sellingPoint: string; spuSeries: string; audience: string; scene: string; page: number; pageSize: number }> = {}) {
    const filters = { query, platform, accountType, direction, sellingPoint, spuSeries, audience, scene, ...overrides };
    const nextRequest = buildContentSearchRequest(
      filters,
      overrides.page ?? appliedRequest.page,
      overrides.pageSize ?? appliedRequest.page_size,
    );
    if (JSON.stringify(nextRequest) === JSON.stringify(appliedRequest)) {
      if (contentsReadFailed) retryContentsRead();
      else void contentsQuery.refetch();
      return;
    }
    setAppliedRequest(nextRequest);
  }

  useEffect(() => {
    if (!contentsQuery.data || contentsQuery.isPlaceholderData) return;
    const lastPage = lastPageFor(contentsQuery.data.total, appliedRequest.page_size);
    if (appliedRequest.page > lastPage) {
      const timer = window.setTimeout(() => {
        setAppliedRequest((current) => ({ ...current, page: lastPage }));
      }, 0);
      return () => window.clearTimeout(timer);
    }
  }, [appliedRequest.page, appliedRequest.page_size, contentsQuery.data, contentsQuery.isPlaceholderData]);

  async function invalidateContentData() {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.contents }),
      queryClient.invalidateQueries({ queryKey: queryKeys.accounts }),
      queryClient.invalidateQueries({ queryKey: queryKeys.overview }),
      queryClient.invalidateQueries({ queryKey: queryKeys.activeSellingPoints, exact: true }),
      queryClient.invalidateQueries({ queryKey: queryKeys.spu }),
    ]);
  }
  const sellingPointSelectRef = useRef<HTMLSelectElement | null>(null);
  const sellingPointLabel = sellingPoint === "" ? "全部卖点" : sellingPoint === "__none__" ? "资料不足" : (() => { const point = sellingPoints.find((item) => item.code === sellingPoint); return point ? `${point.code} · ${point.label}` : sellingPoint; })();
  useEffect(() => {
    const node = sellingPointSelectRef.current;
    if (!node) return;
    const context = document.createElement("canvas").getContext("2d");
    if (!context) return;
    const style = window.getComputedStyle(node as unknown as Element);
    context.font = `${style.fontStyle} ${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
    node.style.width = `${Math.ceil(context.measureText(sellingPointLabel).width) + 46}px`;
  }, [sellingPointLabel, contentsQuery.isPending]);
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
      const wasEdit = Boolean(form.id); setForm(null); setOriginalForm(null); await invalidateContentData(); feedback("", wasEdit ? "内容已更新" : "内容已新增");
    } catch (reason) { feedback(reason instanceof Error ? reason.message : "内容保存失败"); }
    finally { setSaving(false); }
  }
  async function importCsv(file: File) {
    setSaving(true); feedback("");
    try {
      const rows = parseCsv(await file.text()); if (!rows.length) throw new Error("文件中没有可导入的数据。");
      const request = { source_name: file.name, rows };
      const validation = await readJson<{ total: number; rejected: number }>("/api/v8/contents/validate", jsonRequest(request));
      if (validation.rejected) throw new Error(`有 ${validation.rejected} 行格式不正确，本次没有导入任何内容。请修改后重试。`);
      const result = await readJson<{ inserted_rows: number; updated_rows: number; rejected_rows: number }>("/api/v8/contents/import", jsonRequest(request));
      await invalidateContentData(); feedback("", `导入完成：新增 ${result.inserted_rows} 条，更新 ${result.updated_rows} 条，${result.rejected_rows} 条未导入。`);
    } catch (reason) { feedback(reason instanceof Error ? reason.message : "内容导入失败"); }
    finally { setSaving(false); }
  }
  async function updateData(item: ContentItem) {
    setSaving(true); feedback("");
    try { const result = await readJson<{ status: string; provider_cost: number }>(`/api/v8/contents/${item.id}/update-data`, { method: "POST" }); await invalidateContentData(); feedback("", `${item.link_id} 的数据${result.status === "succeeded" ? "已更新" : "只更新了一部分"}；本次付费服务费用 $${Number(result.provider_cost).toFixed(3)}`); }
    catch (reason) { feedback(reason instanceof Error ? reason.message : "数据更新失败，请稍后重试。"); }
    finally { setSaving(false); }
  }

  return <AppShell active="contents">
    <Feedback error={error} message={message} onClose={() => feedback("")} />
    {contentsQuery.isError && <Notice tone="error">{contentsQuery.data ? `数据刷新失败，当前显示上次数据。${contentsQuery.error instanceof Error ? contentsQuery.error.message : ""}` : contentsQuery.error instanceof Error ? contentsQuery.error.message : "内容读取失败"}</Notice>}
    {contentsQuery.isPending && !contentsQuery.data && !contentsReadFailed ? <Loading label="正在读取内容库" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">内容资料库</span><h2>发布内容明细</h2><p>更新数据时会同步更新详情、指标以及已保存的视频和图片；重复提醒会指向最早发布的内容。</p></div><div className="placeholder-actions"><button className="primary button-link" onClick={() => edit()}>新增内容</button><a className="secondary button-link" href={`${API_BASE}/api/v8/contents/export`}>下载内容表格</a><label className="secondary button-link">批量导入<input className="file-input" type="file" accept=".csv,text/csv" disabled={saving} onChange={(event) => { const file = event.target.files?.[0]; if (file) void importCsv(file); event.currentTarget.value = ""; }} /></label></div></div>
      <div className="filter-bar"><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="内容编号、标题、账号编号、昵称或链接" onKeyDown={(event) => { if (event.key === "Enter") applySearch({ page: 1 }); }} /><select value={platform} onChange={(event) => { setPlatform(event.target.value); applySearch({ platform: event.target.value, page: 1 }); }}><option value="">全部平台</option>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select><select value={accountType} onChange={(event) => { setAccountType(event.target.value); applySearch({ accountType: event.target.value, page: 1 }); }}><option value="">全部账号类型</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option><option value="unknown">未知</option></select><select value={direction} onChange={(event) => { setDirection(event.target.value); applySearch({ direction: event.target.value, page: 1 }); }}><option value="">全部内容方向</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option><option value="unknown">未知</option></select><select className="selling-point-filter" ref={sellingPointSelectRef} value={sellingPoint} onChange={(event) => { setSellingPoint(event.target.value); applySearch({ sellingPoint: event.target.value, page: 1 }); }}><option value="">全部卖点</option><option value="__none__">资料不足</option>{sellingPoints.map((point) => <option key={point.code} value={point.code} title={point.label}>{point.code} · {point.label}</option>)}</select>{spuAssets && <><select value={spuSeries} onChange={(event) => { setSpuSeries(event.target.value); applySearch({ spuSeries: event.target.value, page: 1 }); }}><option value="">全部车系</option><option value="__none__">未归车型</option>{spuAssets.spu.filter((row) => row.is_series_node).map((row) => <option key={row.series_slug} value={row.series_slug}>{row.brand} {row.series}</option>)}</select><select value={audience} onChange={(event) => { setAudience(event.target.value); applySearch({ audience: event.target.value, page: 1 }); }}><option value="">全部人群</option><option value="__none__">未归人群</option>{spuAssets.audiences.map((item) => <option key={item.code} value={item.code}>{item.code} {item.label}</option>)}</select><select value={scene} onChange={(event) => { setScene(event.target.value); applySearch({ scene: event.target.value, page: 1 }); }}><option value="">全部场景</option><option value="__none__">未识别场景</option>{spuAssets.scenes.map((item) => <option key={item.code} value={item.code}>{item.code} {item.label}</option>)}</select></>}<button className="secondary" onClick={() => applySearch({ page: 1 })}>搜索</button><span>{contentsReadFailed ? "读取失败" : `${total} 条内容`}</span></div>
      <article className="panel table-panel"><div className="table-scroll"><table className="content-table"><thead><tr><th>内容编号 / 平台作品编号 / 标题</th><th>平台 / 发布时间</th><th>账号</th><th>阅读 / 评论</th><th>账号类型</th><th>内容方向</th><th>车型</th><th>人群</th><th>场景</th><th>卖点</th><th>资料完整度</th><th>垂直度</th><th>拉新</th><th>拉活</th><th>线索</th><th>重复提醒</th><th>操作</th></tr></thead><tbody>{contentsReadFailed ? <tr><td className="table-read-error" colSpan={17}><strong>内容读取失败</strong><span>请检查网络后重新加载。</span><button type="button" className="secondary read-error-retry" disabled={retrying} onClick={retryContentsRead}>{retrying ? "正在重新加载…" : "重新加载"}</button></td></tr> : items.map((item) => <tr key={item.id}><td><a href={item.canonical_url} target="_blank" rel="noreferrer">{item.link_id} · {item.platform_content_id || "平台作品编号缺失"}</a><ContentTitle text={item.title || "标题缺失"} /></td><td>{label(item.platform)}<span className="cell-subline">{formatDateTime(item.published_at)}</span></td><td>{item.raw_account_name || "昵称缺失"}<span className="cell-subline">{item.raw_account_uid || "平台账号编号缺失"}</span></td><td>{item.view_count ?? "暂不可计算"}<span>评论 {item.comment_count ?? "暂不可计算"}</span></td><td>{label(item.account_type)}</td><td>{label(item.content_direction)}</td><td>{item.spu ? <><span className="selling-point-name" title={spuCellTitle(item.spu)}>{spuDisplayName(item.spu)}</span><span className="cell-subline spu-tag-subline" title={spuSubline(item)}>{spuSubline(item)}</span></> : item.spu_gray_count > 0 ? <span className="status-badge pending">车型不确定</span> : "未识别"}</td><td>{item.audience ? <>{item.audience.label}<span className="cell-subline">{label(item.audience.source)}</span></> : "—"}</td><td className="content-scene-cell">{item.scenes && item.scenes.length > 0 ? <>{item.scenes.slice(0, 2).map((sceneTag) => <span className="scene-tag" key={sceneTag.code}>{sceneTag.label}</span>)}{item.scenes.length > 2 && <span className="cell-subline">+{item.scenes.length - 2} 个场景</span>}</> : "—"}</td><td>{item.primary_selling_point_code ? <span className="selling-point-name" title={`${item.primary_selling_point_code} · ${sellingPointLabelByCode.get(item.primary_selling_point_code) ?? "不在当前卖点库"}`}>{sellingPointLabelByCode.get(item.primary_selling_point_code) ?? item.primary_selling_point_code}</span> : item.evidence_level ? "暂不可计算" : "资料不足"}{Boolean(item.evaluation_is_stale) && <span className="status-badge stale-evaluation">结果需更新</span>}</td><td>{item.evidence_level ? <span className="evidence-level-tag" title={EVIDENCE_LEVEL_HINTS[item.evidence_level]}>{EVIDENCE_LEVEL_LABELS[item.evidence_level] ?? item.evidence_level}</span> : "—"}</td><td>{item.content_automotive_score == null ? "暂不可计算" : `${item.content_automotive_score}%`}</td><td>暂不可计算</td><td>暂不可计算</td><td>暂不可计算</td><td>{item.duplicate_original_link_id || "—"}</td><td><span className="row-actions"><button className="text-button" disabled={saving} onClick={() => setEvidenceItem(item)}>查看依据</button><button className="text-button" disabled={saving} onClick={() => void updateData(item)}>更新数据</button><button className="text-button" disabled={saving} onClick={() => edit(item)}>修改</button></span></td></tr>)}</tbody></table></div>
      {!contentsReadFailed && contentsQuery.data && <Pagination page={appliedRequest.page} pageSize={appliedRequest.page_size} total={total} busy={contentsQuery.isFetching || saving} ariaLabel="内容分页" onChange={(next) => applySearch({ page: next.page, pageSize: next.pageSize })} />}
      </article>
    </section>}
    {form && <div className="modal-backdrop" role="presentation"><section className="modal-panel operation-modal" role="dialog" aria-modal="true" aria-label="编辑内容"><div className="panel-head"><div><span className="eyebrow">内容信息</span><h3>{form.id ? "修改内容" : "新增内容"}</h3></div><button className="modal-close" onClick={() => setForm(null)} aria-label="关闭">×</button></div><div className="modal-fields"><label>发布平台<select value={form.platform} onChange={(event) => setForm({ ...form, platform: event.target.value })}>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select></label><label>内容类型<select value={form.contentType} onChange={(event) => setForm({ ...form, contentType: event.target.value })}><option value="video">视频</option><option value="image">图文</option><option value="unknown">未知</option></select></label><label className="span-two">链接<input value={form.canonicalUrl} onChange={(event) => setForm({ ...form, canonicalUrl: event.target.value })} /></label><label>平台内容编号<input value={form.platformContentId} onChange={(event) => setForm({ ...form, platformContentId: event.target.value })} /></label><label>发布日期<input type="datetime-local" value={form.publishedAt} onChange={(event) => setForm({ ...form, publishedAt: event.target.value })} /></label><label className="span-two">标题<input value={form.title} onChange={(event) => setForm({ ...form, title: event.target.value })} /></label><label className="span-two">正文<textarea value={form.body} onChange={(event) => setForm({ ...form, body: event.target.value })} /></label><label>平台账号编号<input value={form.accountUid} onChange={(event) => setForm({ ...form, accountUid: event.target.value })} /></label><label>账号昵称<input value={form.accountName} onChange={(event) => setForm({ ...form, accountName: event.target.value })} /></label><label>账号类型<select value={form.accountType} onChange={(event) => setForm({ ...form, accountType: event.target.value })}><option value="unknown">未知</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option></select></label><label>内容方向<select value={form.contentDirection} onChange={(event) => setForm({ ...form, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label></div><div className="modal-actions"><button className="secondary" onClick={() => setForm(null)}>取消</button><button className="primary" disabled={saving} onClick={() => void save()}>{saving ? "保存中" : "保存内容"}</button></div></section></div>}
    {evidenceItem && <EvidenceModal item={evidenceItem} onClose={() => setEvidenceItem(null)} onChanged={invalidateContentData} onFeedback={feedback} />}
  </AppShell>;
}
