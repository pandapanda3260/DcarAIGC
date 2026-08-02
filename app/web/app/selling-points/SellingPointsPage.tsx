"use client";

import { useEffect, useMemo, useState } from "react";
import AppShell from "../components/AppShell";
import { Feedback, Loading } from "../components/Feedback";
import { jsonRequest, readJson } from "../lib/api";
import { label } from "../lib/format";
import type { SellingPoint, SellingPointResponse } from "../lib/types";

type PointForm = { code: string; tier: "core" | "other"; label: string; definition: string; positiveEvidence: string; negativeEvidence: string; boundaryRules: string; scenes: string[] };
const emptyPoint: PointForm = { code: "", tier: "other", label: "", definition: "", positiveEvidence: "", negativeEvidence: "", boundaryRules: "", scenes: [] };

export default function SellingPointsPage() {
  const [data, setData] = useState<SellingPointResponse>({ taxonomy: null, items: [] });
  const [draftMode, setDraftMode] = useState(false);
  const [form, setForm] = useState<PointForm | null>(null);
  const [editingCode, setEditingCode] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  useEffect(() => { readJson<SellingPointResponse>("/api/v8/selling-points").then(setData).catch((reason) => setError(reason instanceof Error ? reason.message : "卖点标准读取失败")).finally(() => setLoading(false)); }, []);
  const families = useMemo(() => {
    const values = new Map<string, number>();
    data.items.forEach((point) => values.set(point.code.slice(0, 1), (values.get(point.code.slice(0, 1)) ?? 0) + (point.primary_hits ?? 0)));
    return values;
  }, [data]);

  async function beginEdit() {
    setSaving(true); setError("");
    try { await readJson("/api/v8/selling-points/draft", { method: "POST" }); setData(await readJson<SellingPointResponse>("/api/v8/selling-points/draft")); setDraftMode(true); setMessage("已创建隔离草稿；发布前不影响评估"); }
    catch (reason) { setError(reason instanceof Error ? reason.message : "卖点草稿创建失败"); }
    finally { setSaving(false); }
  }
  function open(point?: SellingPoint) {
    setEditingCode(point?.code ?? null);
    setForm(point ? { code: point.code, tier: point.tier === "core" ? "core" : "other", label: point.label, definition: point.definition, positiveEvidence: (point.positive_evidence ?? []).join("\n"), negativeEvidence: (point.negative_evidence ?? []).join("\n"), boundaryRules: (point.boundary_rules ?? []).join("\n"), scenes: point.scenes } : { ...emptyPoint });
  }
  async function save() {
    if (!form) return;
    setSaving(true); setError("");
    const lines = (value: string) => value.split("\n").map((item) => item.trim()).filter(Boolean);
    try {
      await readJson(editingCode ? `/api/v8/selling-points/items/${editingCode}` : "/api/v8/selling-points/items", jsonRequest({ code: form.code, tier: form.tier, label: form.label, definition: form.definition, positive_evidence: lines(form.positiveEvidence), negative_evidence: lines(form.negativeEvidence), boundary_rules: lines(form.boundaryRules), scenes: form.scenes }, editingCode ? "PATCH" : "POST"));
      setData(await readJson<SellingPointResponse>("/api/v8/selling-points/draft")); setForm(null); setMessage("卖点草稿已保存");
    } catch (reason) { setError(reason instanceof Error ? reason.message : "卖点保存失败"); }
    finally { setSaving(false); }
  }
  async function remove(code: string) {
    if (!window.confirm(`确认从草稿删除 ${code}？`)) return;
    setSaving(true); setError("");
    try { await readJson(`/api/v8/selling-points/items/${code}`, { method: "DELETE" }); setData(await readJson<SellingPointResponse>("/api/v8/selling-points/draft")); setMessage(`${code} 已从草稿删除`); }
    catch (reason) { setError(reason instanceof Error ? reason.message : "卖点删除失败"); }
    finally { setSaving(false); }
  }
  async function publish() {
    setSaving(true); setError("");
    try { await readJson("/api/v8/selling-points/publish", { method: "POST" }); setData(await readJson<SellingPointResponse>("/api/v8/selling-points")); setDraftMode(false); setMessage("卖点标准已发布；后续评估使用新版本"); }
    catch (reason) { setError(reason instanceof Error ? reason.message : "卖点标准发布失败"); }
    finally { setSaving(false); }
  }

  return <AppShell active="selling-points">
    <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {loading ? <Loading label="正在读取卖点标准" /> : <section className="page-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">{data.taxonomy?.version ?? "SELLING POINT TAXONOMY"}</span><h2>当前卖点基础标准</h2><p>适用业务场景是集合；草稿增删改不会影响生产评估，只有发布后生效。</p></div><div className="placeholder-actions">{draftMode ? <><button className="secondary" disabled={saving} onClick={() => open()}>新增卖点</button><button className="primary" disabled={saving} onClick={() => void publish()}>发布标准</button></> : <button className="primary" disabled={saving} onClick={() => void beginEdit()}>编辑卖点标准</button>}</div></div>
      <div className="family-strip">{Array.from(families.entries()).map(([family, hits]) => <div key={family}><span>{family} 系列</span><strong>{hits}</strong><small>当前 primary 命中</small></div>)}</div>
      <div className="selling-point-grid">{data.items.map((point) => <article className="panel selling-point-card" key={point.code}><div className="selling-point-code"><strong>{point.code}</strong><span>{point.tier === "core" ? "核心" : "其他"}</span></div><h3>{point.label}</h3><p>{point.definition || "定义以当前词表为准。"}</p><div className="scene-tags">{point.scenes.map((scene) => <span className="scene-tag" key={scene}>{label(scene)}</span>)}</div><footer><span>{draftMode ? "草稿版本，发布前不影响评估" : `primary ${point.primary_hits ?? 0} · 全部命中 ${point.total_hits ?? 0}`}</span><span className="card-actions"><button className="text-button" disabled={!draftMode || saving} onClick={() => open(point)}>编辑</button>{draftMode && <button className="text-button danger" disabled={saving} onClick={() => void remove(point.code)}>删除</button>}</span></footer></article>)}</div>
    </section>}
    {form && <div className="modal-backdrop" role="presentation"><section className="review-modal" role="dialog" aria-modal="true" aria-label="编辑卖点"><div className="panel-head"><div><span className="eyebrow">SELLING POINT DRAFT</span><h3>{editingCode ? `编辑 ${editingCode}` : "新增卖点"}</h3></div><button className="modal-close" onClick={() => setForm(null)} aria-label="关闭">×</button></div><div className="review-fields"><label>编码<input value={form.code} disabled={Boolean(editingCode)} onChange={(event) => setForm({ ...form, code: event.target.value.toUpperCase() })} /></label><label>层级<select value={form.tier} onChange={(event) => setForm({ ...form, tier: event.target.value as PointForm["tier"] })}><option value="core">核心</option><option value="other">其他</option></select></label><label className="span-two">名称<input value={form.label} onChange={(event) => setForm({ ...form, label: event.target.value })} /></label><label className="span-two">定义<textarea value={form.definition} onChange={(event) => setForm({ ...form, definition: event.target.value })} /></label></div><fieldset className="scene-picker"><legend>适用业务场景（至少一项）</legend>{["new_car", "used_car", "media"].map((scene) => <label key={scene}><input type="checkbox" checked={form.scenes.includes(scene)} onChange={(event) => setForm({ ...form, scenes: event.target.checked ? [...form.scenes, scene] : form.scenes.filter((value) => value !== scene) })} />{label(scene)}</label>)}</fieldset><div className="review-fields evidence-fields"><label>正向证据（每行一条）<textarea value={form.positiveEvidence} onChange={(event) => setForm({ ...form, positiveEvidence: event.target.value })} /></label><label>负向证据（每行一条）<textarea value={form.negativeEvidence} onChange={(event) => setForm({ ...form, negativeEvidence: event.target.value })} /></label><label className="span-two">边界规则（每行一条）<textarea value={form.boundaryRules} onChange={(event) => setForm({ ...form, boundaryRules: event.target.value })} /></label></div><div className="modal-actions"><button className="secondary" onClick={() => setForm(null)}>取消</button><button className="primary" disabled={saving || !form.scenes.length} onClick={() => void save()}>{saving ? "保存中" : "保存草稿"}</button></div></section></div>}
  </AppShell>;
}
