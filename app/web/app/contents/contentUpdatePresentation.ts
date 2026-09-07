import { contentUpdateFeedback } from "./contentUpdate";
import { contentUpdateJobBlocksWrites, type ContentUpdateJob, type PendingContentUpdate } from "./contentUpdateJobs";

type ContentUpdateRowState = {
  label: string;
  tone: "active" | "success" | "warning";
  description: string;
};

export function contentUpdateRowState(
  contentId: number,
  jobs: ContentUpdateJob[],
  pending: PendingContentUpdate[],
): ContentUpdateRowState | null {
  const request = pending.filter((item) => item.contentId === contentId)
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt))[0];
  if (request) {
    if (request.status === "submitting") {
      return { label: "正在提交", tone: "active", description: "正在提交更新请求，请勿重复提交。" };
    }
    if (request.status === "uncertain") {
      return { label: "提交待确认", tone: "warning", description: request.error || "提交结果尚未确认，请在更新记录中确认原请求。" };
    }
    return { label: "提交未成功", tone: "warning", description: request.error || "更新请求未提交成功，请在更新记录中查看原因。" };
  }

  const matching = jobs.filter((job) => job.content_id === contentId).sort((a, b) => b.id - a.id);
  // An unresolved request still owns the write lock even if a newer receipt exists.
  const job = matching.find(contentUpdateJobBlocksWrites) ?? matching[0];
  if (!job) return null;
  if (job.error_code === "result_uncertain") {
    return { label: "结果待确认", tone: "warning", description: job.error || "更新结果尚未确认，请在更新记录中查看状态。" };
  }
  if (job.status === "queued") return { label: "排队中", tone: "active", description: job.stage_label || "等待更新" };
  if (job.status === "running") return { label: "更新中", tone: "active", description: job.stage_label || "正在更新数据" };
  if (job.status === "failed") {
    return { label: "更新未完成", tone: "warning", description: job.error || job.stage_label || "更新未完成，请在更新记录中查看原因。" };
  }

  // The job completing does not imply that the underlying data update succeeded.
  const feedback = contentUpdateFeedback(job.result ?? { status: "unknown" });
  if (feedback.error) return { label: "结果待确认", tone: "warning", description: feedback.error };
  if (feedback.message.includes("未全部完成")) {
    return { label: "部分更新", tone: "warning", description: feedback.message };
  }
  return { label: "已更新", tone: "success", description: feedback.message };
}
