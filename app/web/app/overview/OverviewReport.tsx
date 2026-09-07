"use client";

import Image from "next/image";
import { useState, type CSSProperties } from "react";
import { CaretDownIcon, CaretUpIcon, InfoIcon } from "@phosphor-icons/react";
import { metricEvidence, metricStatus, metricUnavailableLabel } from "../lib/format";
import { publicAssetPath } from "../lib/paths";
import type { BusinessSceneKey, Metric } from "../lib/types";
import {
  contentStructure, numberText, overviewMetrics, percentageNumber, percentageText,
  progressWidth, ratioEvidence, sortedSellingPoints, visibleMetricNumber,
  type OverviewSellingPoint, type ReportChannel,
} from "./overviewModel";
import styles from "./OverviewReport.module.css";

const sceneOrder: BusinessSceneKey[] = ["used_car", "new_car", "media"];

function Progress({ metric, core }: { metric: Metric; core?: Metric }) {
  return <span className={styles.track} aria-hidden="true">
    <span className={styles.fill} style={{ width: progressWidth(visibleMetricNumber(metric)) }} />
    {core && <span className={styles.coreFill} style={{ width: progressWidth(visibleMetricNumber(core)) }} />}
  </span>;
}

function SummaryMetric({ metric, label, color, exposure }: { metric: Metric; label: string; color: string; exposure: boolean }) {
  const value = visibleMetricNumber(metric);
  const evidence = metricEvidence(metric);
  return <article className={`${styles.kpi} ${value == null ? styles.metricUnavailable : ""}`}
    style={{ "--metric-color": `var(--ov-${color})` } as CSSProperties}
    aria-label={label} title={evidence}>
    <span className={styles.metricLabel}>{label}</span>
    <strong className={styles.metricValue}>{value == null ? "—" : <>{percentageNumber(value)}<small>%</small></>}</strong>
    <span className={styles.metricEvidence}>{ratioEvidence(metric, exposure) ?? metricUnavailableLabel(metric)}</span>
    {value != null && <Progress metric={metric} />}
    {metric.status === "sample_only" && <span className={styles.metricStatus}>{metricStatus(metric)}</span>}
    <span className="visually-hidden">{evidence}</span>
  </article>;
}

function ContentStructure({ channel }: { channel: ReportChannel }) {
  const structure = contentStructure(channel);
  return <section className={styles.structure} aria-label={`${channel.label}内容结构`}>
    <div className={styles.blockHeader}><h3>内容结构</h3><span>按发布条数</span></div>
    {structure ? <>
      <div className={styles.donutLayout}>
        <div className={styles.donut} style={{ "--core-pct": progressWidth(structure.corePercentage),
          "--selling-pct": progressWidth(structure.sellingPercentage) } as CSSProperties}
          role="img" aria-label={`共 ${structure.total} 条，核心卖点 ${structure.core} 条，其他卖点 ${structure.other} 条，其余内容 ${structure.rest} 条`}>
          <div className={styles.donutCenter}><strong>{percentageText(structure.sellingPercentage)}</strong><span>卖点覆盖</span></div>
        </div>
        <div className={styles.legend}>{([
          ["核心卖点", structure.core, "core"], ["其他卖点", structure.other, "other"], ["其余内容", structure.rest, "rest"],
        ] as const).map(([label, count, color]) => <div className={styles.legendRow} key={label}>
          <span className={styles.legendMark} style={{ background: `var(--ov-${color})` }} aria-hidden="true" />
          <span>{label}</span><strong>{numberText(count)}</strong>
        </div>)}</div>
      </div>
      <div className={styles.structureNote}><span>已计入卖点 <strong>{numberText(structure.selling)} 条</strong></span><span>其余含待评估内容</span></div>
    </> : <p className={styles.emptyState}>{channel.publication_count === 0 ? "所选时间内没有发布内容" : "内容结构暂不可用"}</p>}
  </section>;
}

function SceneMetric({ label, metric, core }: { label: "条数" | "曝光"; metric: Metric; core: Metric }) {
  return <div className={styles.sceneMetric} title={`${metricEvidence(metric)}；${metricEvidence(core)}`}>
    <span>{label}</span><Progress metric={metric} core={core} />
    <span className={styles.sceneValue}>
      <span aria-label={`卖点${label}占比`}>{percentageText(visibleMetricNumber(metric))}</span>
      <span aria-hidden="true"> / </span>
      <span className={styles.coreValue} aria-label={`核心卖点${label}占比`}>{percentageText(visibleMetricNumber(core))}</span>
    </span>
    <span className="visually-hidden">{metricEvidence(metric)}；{metricEvidence(core)}</span>
  </div>;
}

function BusinessScenes({ channel }: { channel: ReportChannel }) {
  return <section className={styles.scenes} aria-label={`${channel.label}三个业务场景`}>
    <div className={styles.blockHeader}><h3>三个业务场景</h3><span>占该渠道总量</span></div>
    {sceneOrder.map((key) => {
      const scene = channel.scenes[key];
      return <article className={styles.scene} key={key}>
        <div className={styles.sceneHeader}><h4>{scene.label}</h4><span>{numberText(scene.publication_count)} 条发布</span></div>
        <SceneMetric label="条数" metric={scene.metrics.selling_point_count_share} core={scene.metrics.core_selling_point_count_share} />
        <SceneMetric label="曝光" metric={scene.metrics.selling_point_exposure_share} core={scene.metrics.core_selling_point_exposure_share} />
      </article>;
    })}
    <div className={styles.legendInline}><span><i style={{ background: "var(--ov-blue)" }} />全部卖点</span><span><i style={{ background: "var(--ov-amber)" }} />其中核心卖点</span></div>
  </section>;
}

function viewEvidence(point: OverviewSellingPoint) {
  const views = visibleMetricNumber(point.view_count);
  const parts = [views == null ? metricEvidence(point.view_count)
    : `所选内容截至采集时的累计 VV：${numberText(views)}`];
  if (point.provided_view_items != null) parts.push(`已取得曝光 ${point.provided_view_items}/${point.publication_count} 条`);
  if (point.missing_view_items) parts.push(`缺失 ${point.missing_view_items} 条`);
  if (point.stale_view_items) parts.push(`需更新 ${point.stale_view_items} 条`);
  return parts.join("；");
}

function PointRow({ point }: { point: OverviewSellingPoint }) {
  const views = visibleMetricNumber(point.view_count);
  const countShare = visibleMetricNumber(point.count_share);
  const exposureShare = visibleMetricNumber(point.exposure_share);
  const evidence = viewEvidence(point);
  return <tr>
    <td><span className={styles.pointLabel}><span className={styles.pointCode}>{point.code_missing ? "待核对" : point.code}</span><span title={point.label}>{point.label}</span></span></td>
    <td className={styles.num}>{numberText(point.publication_count)}</td>
    <td className={styles.num} title={evidence}>{numberText(views)}
      {(views == null || point.stale_view_items > 0 || point.missing_view_items > 0 || point.view_count.status === "sample_only")
        && <span className={styles.rowStatus}>{views == null ? metricUnavailableLabel(point.view_count) : `已取得 ${point.provided_view_items}/${point.publication_count} 条`}</span>}
      <span className="visually-hidden">{evidence}</span>
    </td>
    <td title={`${metricEvidence(point.count_share)}；${metricEvidence(point.exposure_share)}`}>
      <div className={styles.barPair}><span className={styles.pairLines} aria-hidden="true">
        <span style={{ width: progressWidth(countShare) }} /><span style={{ width: progressWidth(exposureShare) }} />
      </span><span className={styles.pairValues}>{percentageText(countShare)} / <strong>{percentageText(exposureShare)}</strong></span></div>
    </td>
  </tr>;
}

function SellingPointTable({ channel }: { channel: ReportChannel }) {
  const [expanded, setExpanded] = useState(false);
  const points = channel.selling_points;
  if (!points) return <section className={styles.detail}><h3>卖点表现</h3><p className={styles.emptyState}>卖点明细暂不可用</p></section>;
  if (points.length === 0) return <section className={styles.detail}><h3>卖点表现</h3><p className={styles.emptyState}>所选时间内没有可计入的卖点内容</p></section>;
  const sorted = sortedSellingPoints(points);
  const visible = expanded ? sorted.items : sorted.items.slice(0, 3);
  const tableId = `overview-selling-points-${channel.platform}`;
  return <section className={styles.detail}>
    <div className={styles.detailHeader}><div className={styles.detailTitle}><h3>卖点表现</h3><span>{sorted.ordering} · 展示{expanded ? "全部" : "前"} {visible.length} 项</span></div>
      {points.length > 3 && <button type="button" className={styles.toggle} aria-controls={tableId} aria-expanded={expanded} onClick={() => setExpanded((value) => !value)}>
        {expanded ? "收起明细" : `查看全部 ${points.length} 项`}{expanded ? <CaretUpIcon size={13} /> : <CaretDownIcon size={13} />}
      </button>}
    </div>
    <div className={styles.tableScroll}><table className={styles.table} id={tableId} aria-label={`${channel.label}卖点表现`}>
      <thead><tr><th>卖点</th><th className={styles.num}>内容数</th><th className={styles.num}>累计 VV</th><th>内容占比 / 曝光占比</th></tr></thead>
      <tbody>{visible.map((point) => <PointRow key={point.code} point={point} />)}</tbody>
    </table></div>
    <p className="visually-hidden" aria-live="polite">{channel.label}卖点明细显示 {visible.length} 项，共 {points.length} 项。</p>
    <div className={styles.tableFooter}><div className={styles.legendInline}><span><i style={{ background: "var(--ov-rest)" }} />内容占比</span><span><i style={{ background: "var(--ov-teal)" }} />曝光占比</span></div>
      <span>占比按渠道总量计算 · 曝光缺失时不据此判断效果</span>
    </div>
  </section>;
}

export function OverviewChannelReport({ channel }: { channel: ReportChannel }) {
  const exposure = channel.summary.metrics.selling_point_exposure_share;
  const exposureUnavailable = visibleMetricNumber(exposure) == null;
  return <section className={styles.channel} data-channel={channel.platform}>
    <header className={styles.header}><div className={styles.heading}>
      <Image className={styles.platform} src={publicAssetPath(channel.platform === "douyin" ? "/brand-douyin-tiktok.svg" : "/brand-xiaohongshu.svg")} alt="" width={30} height={30} unoptimized />
      <h2>{channel.label}渠道</h2></div><span className={styles.publication}><strong>{numberText(channel.publication_count)}</strong> 条发布</span>
    </header>
    <div className={styles.meta}><span>可评估内容 <b>{percentageText(channel.evidence_coverage_percentage)}</b></span><span>有曝光数据 <b>{channel.platform === "xiaohongshu" ? "—" : `${numberText(channel.valid_exposure_items)} 条`}</b></span><span>已完成曝光分类 <b>{percentageText(channel.exposure_coverage_percentage)}</b></span></div>
    <div className={styles.kpis}>{overviewMetrics.map(([key, label, color], index) => <SummaryMetric key={key} metric={channel.summary.metrics[key]} label={label} color={color} exposure={index >= 2} />)}</div>
    <div className={styles.visuals}><ContentStructure channel={channel} /><BusinessScenes channel={channel} /></div>
    {exposureUnavailable && <p className={styles.notice}><InfoIcon size={15} aria-hidden="true" /><span>{channel.platform === "xiaohongshu" ? "小红书接口未提供阅读数，曝光指标暂不可用。" : metricEvidence(exposure)}</span></p>}
    <SellingPointTable channel={channel} />
    <p className={styles.dataNote}>条数占比按该渠道全部发布内容计算；曝光占比仅统计正值 VV，为所选内容截至采集时的累计值。</p>
  </section>;
}
