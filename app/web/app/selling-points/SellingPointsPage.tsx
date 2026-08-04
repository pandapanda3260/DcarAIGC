"use client";

import { useEffect, useState } from "react";
import {
  CarIcon,
  GameControllerIcon,
  PencilSimpleIcon,
  TelevisionIcon,
} from "@phosphor-icons/react";
import AppShell from "../components/AppShell";
import { Feedback, Loading } from "../components/Feedback";
import { jsonRequest, readJson } from "../lib/api";
import { label } from "../lib/format";
import type { BusinessSceneKey, SellingPoint, SellingPointResponse } from "../lib/types";

type PointForm = {
  code: string;
  tier: "core" | "other";
  label: string;
  definition: string;
  matcherRuleJson: string;
};

type StandardFamilyCode = "E" | "X" | "M";
type StandardFamily = {
  code: StandardFamilyCode;
  title: string;
  description: string;
  scene: BusinessSceneKey;
};

const emptyPoint: PointForm = {
  code: "",
  tier: "other",
  label: "",
  definition: "",
  matcherRuleJson: "{}",
};

const standardFamilies = [
  { code: "E", title: "二手车", description: "交易、车况、估值与保障标准", scene: "used_car" },
  { code: "X", title: "新车", description: "选买、测评、价格与交付标准", scene: "new_car" },
  { code: "M", title: "媒体", description: "媒体内容、服务与 AI 小懂标准", scene: "media" },
] as const satisfies ReadonlyArray<StandardFamily>;

function pointsForFamily(items: SellingPoint[], scene: BusinessSceneKey) {
  return items
    .filter((point) => point.scenes.includes(scene))
    .sort((left, right) => left.code.localeCompare(right.code, "en", { numeric: true }));
}

function sceneHits(point: SellingPoint, scene: BusinessSceneKey) {
  return point.scene_hits?.[scene];
}

function FamilyIcon({ code, size = 22 }: { code: StandardFamilyCode; size?: number }) {
  const props = { size, weight: "regular" as const, "aria-hidden": true };
  if (code === "E") return <CarIcon {...props} />;
  if (code === "X") return <GameControllerIcon {...props} />;
  return <TelevisionIcon {...props} />;
}

export default function SellingPointsPage() {
  const [data, setData] = useState<SellingPointResponse>({ taxonomy: null, items: [] });
  const [draftMode, setDraftMode] = useState(false);
  const [form, setForm] = useState<PointForm | null>(null);
  const [editingCode, setEditingCode] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  useEffect(() => {
    readJson<SellingPointResponse>("/api/v8/selling-points")
      .then(setData)
      .catch((reason) => setError(reason instanceof Error ? reason.message : "卖点标准读取失败"))
      .finally(() => setLoading(false));
  }, []);

  async function beginEdit() {
    setSaving(true);
    setError("");
    try {
      await readJson("/api/v8/selling-points/draft", { method: "POST" });
      setData(await readJson<SellingPointResponse>("/api/v8/selling-points/draft"));
      setDraftMode(true);
      setMessage("已创建隔离草稿；原子激活前不影响生产评估");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "卖点草稿创建失败");
    } finally {
      setSaving(false);
    }
  }

  function open(point?: SellingPoint) {
    setEditingCode(point?.code ?? null);
    setForm(point ? {
      code: point.code,
      tier: point.tier === "core" ? "core" : "other",
      label: point.label,
      definition: point.definition,
      matcherRuleJson: JSON.stringify(point.matcher_rule ?? {}, null, 2),
    } : { ...emptyPoint });
  }

  async function save() {
    if (!form) return;
    setSaving(true);
    setError("");
    try {
      let matcherRule: unknown;
      try {
        matcherRule = JSON.parse(form.matcherRuleJson);
      } catch {
        throw new Error("匹配规则 JSON 格式无效");
      }
      if (!matcherRule || typeof matcherRule !== "object" || Array.isArray(matcherRule)) {
        throw new Error("匹配规则必须是一个 JSON 对象");
      }
      await readJson(
        editingCode ? `/api/v8/selling-points/items/${editingCode}` : "/api/v8/selling-points/items",
        jsonRequest({
          code: form.code,
          tier: form.tier,
          label: form.label,
          definition: form.definition,
          matcher_rule: matcherRule,
        }, editingCode ? "PATCH" : "POST"),
      );
      setData(await readJson<SellingPointResponse>("/api/v8/selling-points/draft"));
      setForm(null);
      setMessage("卖点草稿已保存");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "卖点保存失败");
    } finally {
      setSaving(false);
    }
  }

  async function remove(code: string) {
    if (!window.confirm(`确认从草稿删除 ${code}？`)) return;
    setSaving(true);
    setError("");
    try {
      await readJson(`/api/v8/selling-points/items/${code}`, { method: "DELETE" });
      setData(await readJson<SellingPointResponse>("/api/v8/selling-points/draft"));
      setMessage(`${code} 已从草稿删除`);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "卖点删除失败");
    } finally {
      setSaving(false);
    }
  }

  const projectionPoint = editingCode
    ? data.items.find((point) => point.code === editingCode) ?? null
    : null;

  const shellActions = draftMode ? (
    <>
      <span className="selling-point-activation-note" role="status">
        草稿须完成评估回填与验收后，由发布流程原子激活
      </span>
      <button className="secondary selling-point-top-action" disabled={saving} onClick={() => open()}>
        新增卖点
      </button>
    </>
  ) : (
    <button className="secondary selling-point-edit-standard" disabled={loading || saving} onClick={() => void beginEdit()}>
      <PencilSimpleIcon size={16} weight="bold" aria-hidden />
      编辑卖点标准
    </button>
  );

  return (
    <AppShell active="selling-points" actions={shellActions}>
      <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
      {loading ? <Loading label="正在读取卖点标准" /> : (
        <section className="page-stack selling-points-page">
          <section className="selling-point-summary" aria-labelledby="selling-point-summary-title">
            <header className="selling-point-summary-head">
              <h2 id="selling-point-summary-title">当前卖点基础标准</h2>
            </header>
            <div className="selling-point-summary-grid">
              {standardFamilies.map((family) => {
                const familyPoints = pointsForFamily(data.items, family.scene);
                const familyHasHits = familyPoints.length > 0 && familyPoints.every((point) => {
                  const hits = sceneHits(point, family.scene);
                  return typeof hits?.primary_hits === "number" && typeof hits.total_hits === "number";
                });
                const primaryHits = familyPoints.reduce(
                  (sum, point) => sum + (sceneHits(point, family.scene)?.primary_hits ?? 0),
                  0,
                );
                return (
                  <article className="selling-point-summary-item" data-family={family.code} key={family.code}>
                    <div className="selling-point-summary-copy">
                      <span><b>{family.code}</b> {family.title}</span>
                      <strong>{draftMode || !familyHasHits ? "—" : primaryHits.toLocaleString("zh-CN")}</strong>
                      <small>{draftMode ? "草稿不含命中统计" : familyHasHits ? `primary 命中 · ${familyPoints.length} 项标准` : `${familyPoints.length} 项标准`}</small>
                    </div>
                    <span className="selling-point-family-icon"><FamilyIcon code={family.code} size={26} /></span>
                  </article>
                );
              })}
            </div>
          </section>

          <div className="selling-point-family-list">
            {standardFamilies.map((family) => {
              const familyPoints = pointsForFamily(data.items, family.scene);
              const familyHasHits = familyPoints.length > 0 && familyPoints.every((point) => {
                const hits = sceneHits(point, family.scene);
                return typeof hits?.primary_hits === "number" && typeof hits.total_hits === "number";
              });
              const primaryHits = familyPoints.reduce(
                (sum, point) => sum + (sceneHits(point, family.scene)?.primary_hits ?? 0),
                0,
              );
              return (
                <section className="selling-point-family" data-family={family.code} key={family.code} aria-labelledby={`selling-point-family-${family.code}`}>
                  <header className="selling-point-family-head">
                    <div className="selling-point-family-title">
                      <span className="selling-point-family-icon"><FamilyIcon code={family.code} /></span>
                      <div>
                        <h2 id={`selling-point-family-${family.code}`}><b>{family.code}</b> {family.title}</h2>
                      </div>
                    </div>
                    <p className="selling-point-family-meta">
                      {familyPoints.length} 个一级类目
                      <span aria-hidden>·</span>
                      {draftMode ? "草稿版本" : familyHasHits ? `${primaryHits.toLocaleString("zh-CN")} 次 primary 命中` : "暂无命中统计"}
                    </p>
                  </header>
                  <div className="selling-point-table-wrap" role="region" aria-label={`${family.code} ${family.title}卖点标准表格`} tabIndex={0}>
                    <table className="selling-point-table">
                      <caption className="visually-hidden">{family.code} {family.title}卖点标准</caption>
                      <colgroup>
                        <col className="selling-point-code-col" />
                        <col className="selling-point-label-col" />
                        <col className="selling-point-scope-col" />
                        <col className="selling-point-hit-col" />
                        {draftMode && <col className="selling-point-action-col" />}
                      </colgroup>
                      <thead>
                        <tr>
                          <th scope="col">一级类目</th>
                          <th scope="col">卖点标准</th>
                          <th scope="col">层级与适用范围</th>
                          <th scope="col">命中统计</th>
                          {draftMode && <th scope="col">操作</th>}
                        </tr>
                      </thead>
                      <tbody>
                        {familyPoints.map((point) => {
                          const pointSceneHits = sceneHits(point, family.scene);
                          return (
                            <tr key={point.code}>
                              <th scope="row"><span className="selling-point-code-pill">{point.code}</span></th>
                              <td>
                                <div className="selling-point-standard-copy">
                                  <strong>{point.label}</strong>
                                  {point.definition && <span>{point.definition}</span>}
                                </div>
                              </td>
                              <td>
                                <div className="selling-point-scope">
                                  <span className="selling-point-tier">{point.tier === "core" ? "核心" : "其他"}</span>
                                  <span className="selling-point-scene-list">
                                    {point.scenes.map((pointScene) => <span className="scene-tag" key={pointScene}>{label(pointScene)}</span>)}
                                  </span>
                                </div>
                              </td>
                              <td>
                                {draftMode || !pointSceneHits ? <span className="selling-point-hit-empty">—</span> : (
                                  <span className="selling-point-hit-value">
                                    <strong>{pointSceneHits.primary_hits.toLocaleString("zh-CN")}</strong>
                                    <small>全部 {pointSceneHits.total_hits.toLocaleString("zh-CN")}</small>
                                  </span>
                                )}
                              </td>
                              {draftMode && (
                                <td>
                                  <span className="selling-point-row-actions">
                                    <button className="text-button" disabled={saving} onClick={() => open(point)}>编辑</button>
                                    <button className="text-button danger" disabled={saving} onClick={() => void remove(point.code)}>删除</button>
                                  </span>
                                </td>
                              )}
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </section>
              );
            })}
          </div>
        </section>
      )}

      {form && (
        <div className="modal-backdrop" role="presentation">
          <section className="review-modal selling-point-rule-modal" role="dialog" aria-modal="true" aria-label="编辑卖点">
            <div className="panel-head">
              <div><span className="eyebrow">SELLING POINT DRAFT</span><h3>{editingCode ? `编辑 ${editingCode}` : "新增卖点"}</h3></div>
              <button className="modal-close" onClick={() => setForm(null)} aria-label="关闭">×</button>
            </div>
            <div className="review-fields">
              <label>编码<input value={form.code} disabled={Boolean(editingCode)} onChange={(event) => setForm({ ...form, code: event.target.value.toUpperCase() })} /></label>
              <label>层级<select value={form.tier} onChange={(event) => setForm({ ...form, tier: event.target.value as PointForm["tier"] })}><option value="core">核心</option><option value="other">其他</option></select></label>
              <label className="span-two">名称<input value={form.label} onChange={(event) => setForm({ ...form, label: event.target.value })} /></label>
              <label className="span-two">定义<textarea value={form.definition} onChange={(event) => setForm({ ...form, definition: event.target.value })} /></label>
              <label className="span-two">
                匹配规则 JSON
                <textarea
                  className="matcher-rule-editor"
                  value={form.matcherRuleJson}
                  spellCheck={false}
                  onChange={(event) => setForm({ ...form, matcherRuleJson: event.target.value })}
                />
              </label>
            </div>
            <section className="selling-point-projection-preview" aria-label="规则只读投影">
              <header>
                <strong>规则只读投影</strong>
                <span>适用场景与证据说明由匹配规则生成，保存后由服务端重新校验并投影。</span>
              </header>
              {projectionPoint ? (
                <div className="selling-point-projection-grid">
                  <div>
                    <h4>适用业务场景</h4>
                    <p>{projectionPoint.scenes.length ? projectionPoint.scenes.map(label).join("、") : "无"}</p>
                  </div>
                  <div>
                    <h4>正向证据</h4>
                    <p>{projectionPoint.positive_evidence.length ? projectionPoint.positive_evidence.join("；") : "无"}</p>
                  </div>
                  <div>
                    <h4>负向证据</h4>
                    <p>{projectionPoint.negative_evidence.length ? projectionPoint.negative_evidence.join("；") : "无"}</p>
                  </div>
                  <div>
                    <h4>边界规则</h4>
                    <p>{projectionPoint.boundary_rules.length ? projectionPoint.boundary_rules.join("；") : "无"}</p>
                  </div>
                </div>
              ) : (
                <p className="selling-point-projection-empty">新卖点保存并通过规则校验后生成投影。</p>
              )}
            </section>
            <div className="modal-actions">
              <button className="secondary" onClick={() => setForm(null)}>取消</button>
              <button className="primary" disabled={saving || !form.code.trim() || !form.matcherRuleJson.trim()} onClick={() => void save()}>{saving ? "保存中" : "保存草稿"}</button>
            </div>
          </section>
        </div>
      )}
    </AppShell>
  );
}
