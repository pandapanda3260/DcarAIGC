"use client";

import { useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ClockIcon, ShieldCheckIcon } from "@phosphor-icons/react";
import AppShell from "../components/AppShell";
import { Loading, ReadErrorState } from "../components/Feedback";
import { formatDate, formatDateTime } from "../lib/format";
import { overviewQueryOptions } from "../lib/queries";
import type { OverviewChannelKey, WindowKey } from "../lib/types";
import { OverviewChannelReport } from "./OverviewReport";
import styles from "./OverviewReport.module.css";

const windowLabels: Record<WindowKey, string> = {
  yesterday: "昨天", this_week: "本周", last_week: "上周",
};
const channelOrder: OverviewChannelKey[] = ["douyin", "xiaohongshu"];

export default function OverviewPage() {
  const [windowKey, setWindowKey] = useState<WindowKey>("last_week");
  const [retrying, setRetrying] = useState(false);
  const retryInFlight = useRef(false);
  const overviewQuery = useQuery(overviewQueryOptions());
  const overview = overviewQuery.data;
  const readFailed = overviewQuery.isError || retrying;

  function retryOverviewRead() {
    if (retryInFlight.current || overviewQuery.isFetching) return;
    retryInFlight.current = true;
    setRetrying(true);
    void overviewQuery.refetch({ cancelRefetch: false }).finally(() => {
      retryInFlight.current = false;
      setRetrying(false);
    });
  }

  const activeWindow = overview?.windows[windowKey];
  const windowSwitch = <div className={`channel-switch ${styles.windowSwitch}`} role="group" aria-label="统计窗口">
    {(Object.keys(windowLabels) as WindowKey[]).map((key) => <button key={key} type="button" aria-pressed={windowKey === key} className={windowKey === key ? "active" : ""} onClick={() => setWindowKey(key)}>{windowLabels[key]}</button>)}
  </div>;
  return (
    <AppShell active="overview" actions={windowSwitch}>
      {readFailed && <article className="panel"><ReadErrorState
        title={overview ? "数据刷新失败，当前显示上次数据。" : "概览读取失败"}
        description={overviewQuery.error instanceof Error ? overviewQuery.error.message : "请稍后重新加载。"}
        retrying={retrying || overviewQuery.isFetching}
        onRetry={retryOverviewRead}
      /></article>}
      {overviewQuery.isPending && !overview && !readFailed && <Loading label="正在加载运营数据" />}
      {overview && (
        <section className="page-stack overview-dashboard">
          <h2 className="visually-hidden">渠道结论</h2>
          <p className="visually-hidden" aria-live="polite">已切换到{windowLabels[windowKey]}，数据已更新</p>
          {activeWindow && channelOrder.map((key) => <OverviewChannelReport key={`${windowKey}-${key}`} channel={activeWindow.channels[key]} />)}
          <div className={styles.support}>
            <article className={styles.supportPanel}>
              <div className={styles.supportTitle}><ClockIcon size={19} weight="regular" aria-hidden="true" /><h3>{windowLabels[windowKey]}统计时间范围</h3></div>
              <dl className={styles.definition}>
                <div><dt>开始</dt><dd>{activeWindow ? formatDate(activeWindow.period_start) : "—"}</dd></div>
                <div><dt>{windowKey === "this_week" ? "统计截止（北京时间）" : "统计到此日期前一天"}</dt><dd>{activeWindow ? (windowKey === "this_week" ? formatDateTime(activeWindow.period_end) : formatDate(activeWindow.period_end)) : "—"}</dd></div>
                <div><dt>所选时间内发布</dt><dd>{activeWindow?.metrics.publication_count?.value ?? "—"} 条</dd></div>
                <div><dt>可自动评估的内容</dt><dd>{activeWindow?.eligible_count ?? "—"} 条</dd></div>
                <div><dt>未关联账号内容</dt><dd>{activeWindow?.unassociated_content_count ?? "—"} 条</dd></div>
              </dl>
            </article>
            <article className={styles.supportPanel}>
              <div className={styles.supportTitle}><ShieldCheckIcon size={19} weight="regular" aria-hidden="true" /><div><h3>数据质量状态</h3><p>缺日期内容不进入任何日期窗口，重复内容单独记录。</p></div></div>
              <div className={styles.quality}>
                <div><strong>{overview?.data_quality.missing_published_at ?? "—"}</strong><span>缺失发布日期</span></div>
                <div><strong>{overview?.data_quality.duplicate_fingerprint_coverage ?? "—"}%</strong><span>重复内容识别完成率</span></div>
                <div><strong>{overview?.data_quality.confirmed_duplicate_count ?? "—"}</strong><span>确认重复内容</span></div>
                <div><strong>{overview?.data_quality.duplicate_calibration_ready ? "已通过" : "未通过"}</strong><span>重复识别规则校验</span></div>
              </div>
            </article>
          </div>
        </section>
      )}
    </AppShell>
  );
}
