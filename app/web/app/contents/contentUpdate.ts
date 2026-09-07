// Terminal payload from the manual provider, including background update jobs.
// The provider returns succeeded/partial; the job's queued/running states are separate.
// Keep status open for runtime validation so an unexpected response cannot be
// mistaken for an accepted background job or a partially successful update.
export type ContentUpdateResult = {
  status: string;
  provider_cost?: number | null;
  currency?: string;
  stages?: { stage: string; status: string; error_code?: string | null }[];
  metrics?: { status: string; missing_fields?: string[] } | null;
  media?: { status: string; reason?: string | null; can_restore?: boolean } | null;
};

const metricLabels: Record<string, string> = {
  view_count: "阅读数",
  comment_count: "评论数",
  like_count: "点赞数",
  share_count: "分享数",
  collect_count: "收藏数",
};
const stageLabels: Record<string, string> = {
  detail: "内容资料",
  detail_type_probe: "内容类型",
  metrics: "指标",
  comments: "评论资料",
};
const completedStageStatuses = new Set(["succeeded", "replayed", "already_succeeded"]);

function costMessage(result: ContentUpdateResult) {
  const cost = result.provider_cost;
  if (typeof cost !== "number" || !Number.isFinite(cost) || cost < 0) return "本次费用未返回";
  if (cost === 0) return "本次未产生付费服务费用";
  if (result.currency && result.currency !== "USD") return "本次费用币种未能确认";
  return `本次付费服务费用 $${cost.toLocaleString("zh-CN", { minimumFractionDigits: 3, maximumFractionDigits: 6 })}`;
}

function mediaMessage(media: ContentUpdateResult["media"]): string | null {
  if (!media || media.status === "evidence_ready") return "";
  switch (media.status) {
    case "restore_required":
      if (media.reason === "original_restoring") return "媒体原件正在恢复，完成后可重新处理";
      return media.can_restore === true
        ? "媒体原件需恢复，请在查看依据中申请恢复"
        : "媒体原件暂不可用，请在查看依据中查看状态";
    case "expired_non_replayable": return "媒体原件已到保留期限，无法重新处理";
    case "retryable_failed": return "媒体处理失败，可在查看依据中重试";
    case "no_source": return "尚无可处理的媒体资料";
    case "legacy_source_skipped": return "已有媒体未重新处理，请在查看依据中查看状态";
    default: return null;
  }
}

export function contentUpdateFeedback(
  result: ContentUpdateResult,
  linkId?: string,
): { error: string; message: string } {
  const subject = linkId ? `${linkId} 的数据` : "数据";
  const cost = costMessage(result);
  if (result.status === "failed") return { error: `${subject}更新失败；${cost}。`, message: "" };
  if (result.status !== "succeeded" && result.status !== "partial") {
    return { error: `未能确认${subject}更新结果，请重新读取状态；${cost}。`, message: "" };
  }

  const stages = result.stages ?? [];
  const metrics = result.metrics;
  const media = mediaMessage(result.media);
  if (media === null
      || stages.some((stage) => !completedStageStatuses.has(stage.status) && stage.status !== "failed")
      || (metrics && metrics.status !== "succeeded" && metrics.status !== "partial")) {
    return { error: `未能确认${subject}的完整更新结果，请重新读取状态；${cost}。`, message: "" };
  }

  const limitations: string[] = [];
  const missing = [...new Set((metrics?.missing_fields ?? []).map((field) => metricLabels[field] ?? "部分指标"))];
  if (missing.length) limitations.push(`${missing.join("、")}尚无新数据`);
  else if (metrics?.status === "partial") limitations.push("部分指标尚未补齐");
  const failedStages = stages.filter((stage) => stage.status === "failed");
  if (failedStages.length) {
    const names = [...new Set(failedStages.map((stage) => stageLabels[stage.stage] ?? "部分资料"))];
    limitations.push(`${names.join("、")}更新未完成`);
  }
  if (media) limitations.push(media);
  const complete = result.status === "succeeded" && limitations.length === 0;
  return {
    error: "",
    message: [`${subject}${complete ? "已更新" : "更新未全部完成"}`, ...limitations, cost].join("；") + "。",
  };
}
