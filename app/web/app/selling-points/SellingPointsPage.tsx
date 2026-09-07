"use client";

import { Fragment, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  CarIcon,
  GameControllerIcon,
  StarFourIcon,
  TelevisionIcon,
} from "@phosphor-icons/react";
import AppShell from "../components/AppShell";
import { Loading, Notice } from "../components/Feedback";
import { label } from "../lib/format";
import { activeSellingPointsQueryOptions } from "../lib/queries";
import type {
  BusinessSceneKey,
  OverviewChannelKey,
  SellingPoint,
  SellingPointResponse,
  WindowKey,
} from "../lib/types";

type StandardFamilyCode = "E" | "X" | "M";
type StandardFamily = {
  code: StandardFamilyCode;
  title: string;
  description: string;
  scene: BusinessSceneKey;
};

const emptySellingPoints: SellingPointResponse = { taxonomy: null, items: [] };

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
  const currentQuery = useQuery(activeSellingPointsQueryOptions());
  const data = currentQuery.data ?? emptySellingPoints;

  return (
    <AppShell active="selling-points">
      {currentQuery.isError && <Notice tone="error">{currentQuery.data ? `数据刷新失败，当前显示上次数据。${currentQuery.error instanceof Error ? currentQuery.error.message : ""}` : currentQuery.error instanceof Error ? currentQuery.error.message : "卖点标准读取失败"}</Notice>}
      {currentQuery.isPending && !currentQuery.data ? <Loading label="正在读取卖点标准" /> : (
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
                      <strong>{!familyHasHits ? "—" : primaryHits.toLocaleString("zh-CN")}</strong>
                      <small>{familyHasHits ? `主要卖点命中 · ${familyPoints.length} 项标准` : `${familyPoints.length} 项标准`}</small>
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
                        {data.windows
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
                        </tr>
                      </thead>
                      <tbody>
                        {familyPoints.map((point) => {
                          const pointSceneHits = sceneHits(point, family.scene);
                          const pointWindowHits = point.window_hits?.[statWindow]?.[family.scene];
                          const statsReady = Boolean(data.windows);
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
                                ) : pointSceneHits ? (
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
    </AppShell>
  );
}
