"use client";

import { Fragment, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  CarIcon,
  GameControllerIcon,
  PlusIcon,
  StarFourIcon,
  TelevisionIcon,
} from "@phosphor-icons/react";
import AppShell from "../components/AppShell";
import { Feedback, Loading, Notice } from "../components/Feedback";
import { jsonRequest, readJson } from "../lib/api";
import { label } from "../lib/format";
import { activeSellingPointsQueryOptions, draftSellingPointsQueryOptions, queryKeys } from "../lib/queries";
import sellingPointV53 from "../../../../config/business_selling_points_v5_3.json";
import type {
  BusinessSceneKey,
  OverviewChannelKey,
  SellingPoint,
  SellingPointResponse,
  WindowKey,
} from "../lib/types";

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

const emptySellingPoints: SellingPointResponse = { taxonomy: null, items: [] };

const v53PreviewByCode = new Map(
  sellingPointV53.labels.map((point) => [point.id, point] as const),
);

function withV53PreviewCopy(response: SellingPointResponse): SellingPointResponse {
  return {
    ...response,
    items: response.items.map((point) => {
      const preview = v53PreviewByCode.get(point.code);
      return preview ? {
        ...point,
        label: preview.label,
        definition: preview.definition,
      } : point;
    }),
  };
}

const standardFamilies = [
  { code: "E", title: "二手车", description: "交易、车况、估值与保障标准", scene: "used_car" },
  { code: "X", title: "新车", description: "选买、测评、价格与交付标准", scene: "new_car" },
  { code: "M", title: "媒体", description: "媒体内容、服务与 AI 小懂标准", scene: "media" },
] as const satisfies ReadonlyArray<StandardFamily>;

const statWindowLabels: Record<WindowKey, string> = {
  yesterday: "昨天",
  this_week: "本周",
  last_week: "上周",
};

const statChannels = [
  { key: "douyin", label: "抖音" },
  { key: "xiaohongshu", label: "小红书" },
] as const satisfies ReadonlyArray<{ key: OverviewChannelKey; label: string }>;

function formatShare(numerator: number, denominator: number | undefined) {
  if (!denominator) return null;
  return `${((numerator * 100) / denominator).toFixed(1)}%`;
}

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
  const [statWindow, setStatWindow] = useState<WindowKey>("last_week");
  const [draftMode, setDraftMode] = useState(false);
  const [previewMode, setPreviewMode] = useState(true);
  const [form, setForm] = useState<PointForm | null>(null);
  const [editingCode, setEditingCode] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const queryClient = useQueryClient();
  const activeQuery = useQuery({ ...activeSellingPointsQueryOptions(), enabled: !draftMode });
  const draftQuery = useQuery({ ...draftSellingPointsQueryOptions(), enabled: draftMode });
  const currentQuery = draftMode ? draftQuery : activeQuery;
  const currentData = currentQuery.data ?? emptySellingPoints;
  const data = previewMode && !draftMode ? withV53PreviewCopy(currentData) : currentData;

  async function ensureDraft(): Promise<SellingPointResponse> {
    if (draftMode) return queryClient.fetchQuery(draftSellingPointsQueryOptions());
    await readJson("/api/v8/selling-points/draft", { method: "POST" });
    await queryClient.invalidateQueries({ queryKey: queryKeys.draftSellingPoints, exact: true });
    const draft = await queryClient.fetchQuery(draftSellingPointsQueryOptions());
    setDraftMode(true);
    setPreviewMode(false);
    setMessage("已进入草稿编辑。发布前不会影响当前的评估结果。");
    return draft;
  }

  async function beginPoint(point?: SellingPoint) {
    setSaving(true);
    setError("");
    try {
      const draft = await ensureDraft();
      const draftPoint = point ? draft.items.find((item) => item.code === point.code) : undefined;
      if (point && !draftPoint) throw new Error("这个卖点已不在草稿中，请刷新页面后重试。");
      open(draftPoint);
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
        throw new Error("匹配规则填写格式不正确，请检查后重试。");
      }
      if (!matcherRule || typeof matcherRule !== "object" || Array.isArray(matcherRule)) {
        throw new Error("匹配规则最外层需要使用大括号，请检查后重试。");
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
      await queryClient.invalidateQueries({ queryKey: queryKeys.draftSellingPoints, exact: true });
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
      await ensureDraft();
      await readJson(`/api/v8/selling-points/items/${code}`, { method: "DELETE" });
      await queryClient.invalidateQueries({ queryKey: queryKeys.draftSellingPoints, exact: true });
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

  const shellActions = (
    <>
      {draftMode && (
        <span className="selling-point-activation-note" role="status">
          草稿完成检查并发布后才会正式生效
        </span>
      )}
      {!draftMode && previewMode && (
        <span className="selling-point-activation-note" role="status">
          当前仅预览 v5.3 新文案，命中统计和后台评估仍使用 v5.2
        </span>
      )}
      {!draftMode && (
        <button
          className="secondary selling-point-edit-standard"
          disabled={currentQuery.isPending || saving}
          onClick={() => {
            setPreviewMode((value) => !value);
            setMessage(previewMode ? "已切回当前正式生效版 v5.2" : "当前显示 v5.3 新文案预览");
          }}
        >
          {previewMode ? "查看正式生效版" : "预览 v5.3 新文案"}
        </button>
      )}
      <button className="secondary selling-point-edit-standard" disabled={currentQuery.isPending || saving} onClick={() => void beginPoint()}>
        <PlusIcon size={16} weight="bold" aria-hidden />
        新增卖点
      </button>
    </>
  );

  return (
    <AppShell active="selling-points" actions={shellActions}>
      <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
      {currentQuery.isError && <Notice tone="error">{currentQuery.data ? `数据刷新失败，当前显示上次数据。${currentQuery.error instanceof Error ? currentQuery.error.message : ""}` : currentQuery.error instanceof Error ? currentQuery.error.message : "卖点标准读取失败"}</Notice>}
      {currentQuery.isPending && !currentQuery.data ? <Loading label="正在读取卖点标准……" /> : (
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
                    <span className="selling-point-family-icon"><FamilyIcon code={family.code} size={26} /></span>
                    <div className="selling-point-summary-copy">
                      <span><b>{family.code}</b> {family.title}</span>
                      <strong>{draftMode || !familyHasHits ? "—" : primaryHits.toLocaleString("zh-CN")}</strong>
                      <small>{draftMode ? "草稿不含命中统计" : familyHasHits ? `主要卖点命中 · ${familyPoints.length} 项标准` : `${familyPoints.length} 项标准`}</small>
                    </div>
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
              const windowPrimaryHits = familyPoints.reduce(
                (sum, point) => sum + (point.window_hits?.[statWindow]?.[family.scene]?.primary_hits ?? 0),
                0,
              );
              const sceneDenominators = data.windows?.[statWindow]?.scene_denominators?.[family.scene];
              return (
                <section className="selling-point-family" data-family={family.code} key={family.code} aria-labelledby={`selling-point-family-${family.code}`}>
                  <header className="selling-point-family-head">
                    <div className="selling-point-family-title">
                      <span className="selling-point-family-icon"><FamilyIcon code={family.code} /></span>
                      <div>
                        <h2 id={`selling-point-family-${family.code}`}><b>{family.code}</b> {family.title}</h2>
                      </div>
                    </div>
                    <div className="selling-point-family-side">
                      <p className="selling-point-family-meta">
                        {familyPoints.length} 个一级类目
                        <span aria-hidden>·</span>
                        {draftMode
                          ? "草稿版本"
                          : data.windows
                            ? `${statWindowLabels[statWindow]} ${windowPrimaryHits.toLocaleString("zh-CN")} 次主要卖点命中`
                            : familyHasHits
                              ? `${primaryHits.toLocaleString("zh-CN")} 次主要卖点命中`
                              : "暂无命中统计"}
                      </p>
                      <span className="selling-point-window-control">
                        <label htmlFor={`selling-point-window-${family.code}`}>统计窗口</label>
                        <select
                          id={`selling-point-window-${family.code}`}
                          className="selling-point-window-select"
                          value={statWindow}
                          onChange={(event) => setStatWindow(event.target.value as WindowKey)}
                        >
                          {(Object.keys(statWindowLabels) as WindowKey[]).map((key) => (
                            <option key={key} value={key}>{statWindowLabels[key]}</option>
                          ))}
                        </select>
                      </span>
                    </div>
                  </header>
                  <div className="selling-point-table-wrap" role="region" aria-label={`${family.code} ${family.title}卖点标准表格`} tabIndex={0}>
                    <table className="selling-point-table">
                      <caption className="visually-hidden">{family.code} {family.title}卖点标准</caption>
                      <colgroup>
                        <col className="selling-point-code-col" />
                        <col className="selling-point-label-col" />
                        <col className="selling-point-scope-col" />
                        <col className="selling-point-hit-col" />
                        {statChannels.map((channel) => (
                          <Fragment key={channel.key}>
                            <col className="selling-point-share-col" />
                            <col className="selling-point-share-col" />
                          </Fragment>
                        ))}
                        <col className="selling-point-action-col" />
                      </colgroup>
                      <thead>
                        <tr>
                          <th scope="col">一级类目</th>
                          <th scope="col">卖点标准</th>
                          <th scope="col">层级与适用范围</th>
                          <th scope="col">命中统计</th>
                          {statChannels.map((channel) => (
                            <Fragment key={channel.key}>
                              <th scope="col">{channel.label}条数占比</th>
                              <th scope="col">{channel.label}曝光占比</th>
                            </Fragment>
                          ))}
                          <th scope="col">操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        {familyPoints.map((point) => {
                          const pointSceneHits = sceneHits(point, family.scene);
                          const pointWindowHits = point.window_hits?.[statWindow]?.[family.scene];
                          const statsReady = !draftMode && Boolean(data.windows);
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
                                  <span className="selling-point-tier" data-tier={point.tier === "core" ? "core" : "other"}>
                                    {point.tier === "core" && <StarFourIcon size={9} weight="fill" aria-hidden />}
                                    {point.tier === "core" ? "核心" : "其他"}
                                  </span>
                                  <span className="selling-point-scene-list">
                                    {point.scenes.map((pointScene) => <span className="scene-tag" key={pointScene}>{label(pointScene)}</span>)}
                                  </span>
                                </div>
                              </td>
                              <td>
                                {statsReady ? (
                                  <span className="selling-point-hit-value">
                                    <strong>{(pointWindowHits?.primary_hits ?? 0).toLocaleString("zh-CN")}</strong>
                                    <small>全部 {(pointWindowHits?.total_hits ?? 0).toLocaleString("zh-CN")}</small>
                                  </span>
                                ) : !draftMode && pointSceneHits ? (
                                  <span className="selling-point-hit-value">
                                    <strong>{pointSceneHits.primary_hits.toLocaleString("zh-CN")}</strong>
                                    <small>全部 {pointSceneHits.total_hits.toLocaleString("zh-CN")}</small>
                                  </span>
                                ) : <span className="selling-point-hit-empty">—</span>}
                              </td>
                              {statChannels.map((channel) => {
                                const channelHits = pointWindowHits?.channels?.[channel.key];
                                const channelDenominator = sceneDenominators?.[channel.key];
                                const countShare = statsReady
                                  ? formatShare(channelHits?.primary_hits ?? 0, channelDenominator?.publication_count)
                                  : null;
                                const exposureShare = statsReady
                                  ? formatShare(channelHits?.primary_views ?? 0, channelDenominator?.valid_exposure_views)
                                  : null;
                                return (
                                  <Fragment key={channel.key}>
                                    <td>
                                      <span
                                        className={countShare ? "selling-point-share-value" : "selling-point-hit-empty"}
                                        title={statsReady && channelDenominator?.publication_count
                                          ? `${(channelHits?.primary_hits ?? 0).toLocaleString("zh-CN")} / ${channelDenominator.publication_count.toLocaleString("zh-CN")} 条发布`
                                          : statsReady ? `${channel.label}窗口内无发布` : undefined}
                                      >
                                        {countShare ?? "—"}
                                      </span>
                                    </td>
                                    <td>
                                      <span
                                        className={exposureShare ? "selling-point-share-value" : "selling-point-hit-empty"}
                                        title={statsReady && channelDenominator?.valid_exposure_views
                                          ? `${(channelHits?.primary_views ?? 0).toLocaleString("zh-CN")} / ${channelDenominator.valid_exposure_views.toLocaleString("zh-CN")} 次有效曝光`
                                          : statsReady ? `${channel.label}窗口内无有效曝光` : undefined}
                                      >
                                        {exposureShare ?? "—"}
                                      </span>
                                    </td>
                                  </Fragment>
                                );
                              })}
                              <td>
                                <span className="selling-point-row-actions">
                                  <button className="text-button" disabled={saving} onClick={() => void beginPoint(point)}>编辑</button>
                                  <button className="text-button danger" disabled={saving} onClick={() => void remove(point.code)}>删除</button>
                                </span>
                              </td>
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
          <section className="modal-panel selling-point-rule-modal" role="dialog" aria-modal="true" aria-label="编辑卖点">
            <div className="panel-head">
              <div><span className="eyebrow">卖点草稿</span><h3>{editingCode ? `编辑 ${editingCode}` : "新增卖点"}</h3></div>
              <button className="modal-close" onClick={() => setForm(null)} aria-label="关闭">×</button>
            </div>
            <div className="modal-fields">
              <label>编码<input value={form.code} disabled={Boolean(editingCode)} onChange={(event) => setForm({ ...form, code: event.target.value.toUpperCase() })} /></label>
              <label>层级<select value={form.tier} onChange={(event) => setForm({ ...form, tier: event.target.value as PointForm["tier"] })}><option value="core">核心</option><option value="other">其他</option></select></label>
              <label className="span-two">名称<input value={form.label} onChange={(event) => setForm({ ...form, label: event.target.value })} /></label>
              <label className="span-two">定义<textarea value={form.definition} onChange={(event) => setForm({ ...form, definition: event.target.value })} /></label>
              <label className="span-two">
                匹配规则（请按示例格式填写）
                <textarea
                  className="matcher-rule-editor"
                  value={form.matcherRuleJson}
                  spellCheck={false}
                  onChange={(event) => setForm({ ...form, matcherRuleJson: event.target.value })}
                />
              </label>
            </div>
            <section className="selling-point-projection-preview" aria-label="规则效果预览">
              <header>
                <strong>规则效果预览</strong>
                <span>系统会根据匹配规则自动生成适用场景和判断依据，并在保存时再次检查。</span>
              </header>
              {projectionPoint ? (
                <div className="selling-point-projection-grid">
                  <div>
                    <h4>适用业务场景</h4>
                    <p>{projectionPoint.scenes.length ? projectionPoint.scenes.map(label).join("、") : "无"}</p>
                  </div>
                  <div>
                    <h4>会判定为匹配的内容</h4>
                    <p>{projectionPoint.positive_evidence.length ? projectionPoint.positive_evidence.join("；") : "无"}</p>
                  </div>
                  <div>
                    <h4>会排除的内容</h4>
                    <p>{projectionPoint.negative_evidence.length ? projectionPoint.negative_evidence.join("；") : "无"}</p>
                  </div>
                  <div>
                    <h4>特殊情况</h4>
                    <p>{projectionPoint.boundary_rules.length ? projectionPoint.boundary_rules.join("；") : "无"}</p>
                  </div>
                </div>
              ) : (
                <p className="selling-point-projection-empty">保存并通过检查后，这里会显示适用场景和判断依据。</p>
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
