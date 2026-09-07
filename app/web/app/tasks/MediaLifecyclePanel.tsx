"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Pagination } from "../components/Pagination";
import { readQueryJson } from "../lib/api";
import { formatDateTime, label } from "../lib/format";
import { mediaAge, mediaBlockerLabel, mediaBytes, mediaManualPage } from "../lib/mediaLifecycle";
import type { MediaLifecycleSummary } from "../lib/types";

const jobNames: Record<string, string> = {
  media_archive: "原件归档", media_restore: "原件恢复", media_expiry: "到期删除",
  media_lifecycle: "原件生命周期", media_completion: "完成门核验",
  media_lifecycle_retention: "原件保留结算", media_lifecycle_restore: "原件恢复",
};
const statuses: Record<string, string> = {
  pending: "排队中", running: "执行中", succeeded: "完成", partial: "部分完成",
  failed: "失败", retryable_failed: "失败待重试", terminal_failed: "处理终止", skipped: "未执行",
};

export function MediaLifecycleSummaryView({ summary }: { summary: MediaLifecycleSummary }) {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const manual = mediaManualPage(summary.manual_todos, page, pageSize);
  return <article className="panel" aria-label="原件存储与人工待办">
    <div className="panel-head"><div><h3>原件存储与人工待办</h3><p>只统计已登记媒体，不扫描历史缓存，不会调用收费服务。</p></div><span className="rule-chip">{summary.read_only ? "只读快照" : "本地写入服务"}</span></div>
    <p className="evidence-meta">统计时间：{formatDateTime(summary.as_of)} · 最早删除期限：{formatDateTime(summary.earliest_delete_due_at)} · 归档目录：{summary.archive_root_health === "available" ? "可用" : summary.archive_root_health === "not_mounted_in_replica" ? "只在本地挂载" : summary.archive_root_health === "not_activated" ? "未激活" : "需要检查"}</p>
    {summary.snapshot_only && <p className="evidence-meta">快照截止：{summary.snapshot_captured_at ? formatDateTime(summary.snapshot_captured_at) : "未知"} · 快照延迟：{mediaAge(summary.snapshot_lag_seconds)}。此处不是本地归档目录的实时状态。</p>}
    <div className="quality-grid">
      <div><strong>{summary.counts.hot ?? "未知"}</strong><span>热存实例</span></div>
      <div><strong>{summary.counts.archived ?? "未知"}</strong><span>已归档实例</span></div>
      <div><strong>{summary.counts.expired ?? "未知"}</strong><span>原件已删除</span></div>
      <div><strong>{summary.totals.queued ?? "未知"}</strong><span>恢复排队</span></div>
      <div><strong>{summary.totals.expiry_pending ?? "未知"}</strong><span>已到删除时间</span></div>
      <div><strong>{summary.totals.purging ?? "未知"}</strong><span>删除结算中</span></div>
      <div><strong>{summary.totals.protected ?? "未知"}</strong><span>有保护项</span></div>
      <div><strong>{summary.manual_count ?? "未知"}</strong><span>超过 14 天人工待办</span></div>
    </div>
    <p>人工待办登记原件 {mediaBytes(summary.manual_bytes)} · 最长等待 {mediaAge(summary.manual_longest_age_seconds)}。该体积是登记清单口径，不是系统磁盘占用量。</p>
    {(summary.blocker_groups ?? []).length > 0 && <p className="evidence-meta">阻塞分类：{summary.blocker_groups!.map((group) => mediaBlockerLabel(group.category) + " " + group.count + " 个（" + mediaBytes(group.registered_bytes) + "）").join("；")}</p>}
    {manual.total > 0 ? <><div className="table-scroll"><table className="data-table"><thead><tr><th>作品 / 账号</th><th>登记与等待</th><th>原件 / 体积</th><th>阻塞原因</th><th>最近处理</th><th>保护与人工结论</th></tr></thead><tbody>
      {manual.items.map((item) => <tr key={item.bundle_id}>
        <td>{item.link_id}<small>实例 {item.bundle_id.slice(0, 12)}</small><small>{label(item.platform ?? "unknown")} · {item.account_name || item.account_uid || "账号未知"}</small></td>
        <td>{formatDateTime(item.registered_at)}<small>{mediaAge(item.age_seconds)}</small></td>
        <td>{item.member_count ?? "未知"} 份<small>{mediaBytes(item.registered_bytes)}</small></td>
        <td>{item.evidence_ready ? "证据已齐，待人工确认" : item.blockers.length ? item.blockers.map(mediaBlockerLabel).join("；") : "完成门状态待核验"}{item.last_error && <small>{mediaBlockerLabel(item.last_error)}</small>}</td>
        <td>{item.latest_processing ? <>{statuses[item.latest_processing.status] ?? "状态未知"} · {item.latest_processing.attempt_count} 次<small>{formatDateTime(item.latest_processing.updated_at)}</small></> : "暂无处理记录"}</td>
        <td>{item.protected ? "保护中，需人工核查" : "待人工核查"}<small>{item.resolution ? mediaBlockerLabel(item.resolution) : "尚未记录处置意见"}</small></td>
      </tr>)}
    </tbody></table></div><Pagination page={manual.page} pageSize={manual.pageSize} total={manual.total} ariaLabel="原件人工待办分页" onChange={(next) => { setPage(next.page); if (next.pageSize) setPageSize(next.pageSize); }} /></> : <p className="empty-explanation">当前没有超过 14 天仍未完成证据门的实例。</p>}
    <h4>到期删除欠账</h4><p className="evidence-meta">登记体积 {mediaBytes(summary.expiry_debt_bytes)} · 最长逾期 {mediaAge(summary.expiry_debt_longest_overdue_seconds)}。保护、读取或作业失败会延迟结算，但不会延长原定恢复期限。</p>
    {(summary.expiry_debt ?? []).length > 0 ? <div className="table-scroll"><table className="data-table"><thead><tr><th>作品</th><th>原定删除时间</th><th>逾期</th><th>体积</th><th>延迟原因 / 保护</th></tr></thead><tbody>{summary.expiry_debt!.map((item) => <tr key={item.bundle_id}><td>{item.link_id}<small>{item.bundle_id.slice(0, 12)}</small></td><td>{formatDateTime(item.delete_due_at)}</td><td>{mediaAge(item.overdue_seconds)}</td><td>{mediaBytes(item.registered_bytes)}</td><td>{mediaBlockerLabel(item.delay_reason)}{item.protected && " · 有保护项"}{item.last_error && <small>{mediaBlockerLabel(item.last_error)}</small>}</td></tr>)}</tbody></table></div> : <p className="empty-explanation">当前没有登记的到期删除欠账。</p>}
    <h4>最近生命周期作业</h4><div className="slot-list">{summary.latest_jobs.map((job) => <div key={job.id}><strong>{jobNames[job.job_id] ?? "原件生命周期处理"} · #{job.id}</strong><span>{statuses[job.status] ?? "状态未知"} · {formatDateTime(job.completed_at)}</span>{job.reason && <small>{mediaBlockerLabel(job.reason)}</small>}</div>)}</div>{summary.latest_jobs.length === 0 && <p className="empty-explanation">暂无作业回执，不能视作已完成归档或删除。</p>}
    <p className="evidence-meta">仅处理流程启用后的新下载。完成证据核验并验证可恢复后，归档保留 72 小时，然后直接删除；没有回收区。未通过完成门的 14 天待办只由人工处置，不自动归档、删除或解除保护。原件到期不改变已完成分析、预览和报告；付费重新获取必须另建独立授权任务。</p>
  </article>;
}

export default function MediaLifecyclePanel() {
  const query = useQuery({ queryKey: ["media", "lifecycle"],
    queryFn: () => readQueryJson<MediaLifecycleSummary>("/api/v8/media/lifecycle"),
    refetchInterval: (current) => current.state.error ? false : 30000 });
  if (query.data) return <MediaLifecycleSummaryView summary={query.data} />;
  return <article className="panel"><h3>原件存储与人工待办</h3><p>{query.isError ? "原件状态读取失败，不能据此判断是否可恢复；请稍后刷新。" : "正在读取原件与保护项状态。"}</p></article>;
}
