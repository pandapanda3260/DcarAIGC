import type { EvidenceBundle, EvidenceMedia, MediaLifecycleSummary } from "./types";

const titles: Record<string, string> = {
  original_available: "原件可用",
  original_archived: "原件已归档",
  original_restoring: "正在恢复原件",
  original_expiry_pending: "已到删除时间，等待安全删除",
  original_purge_in_progress: "原件正在安全删除",
  original_expired: "原件已到期删除",
  replica_original_omitted: "线上只读副本不包含原件",
  original_missing: "原件意外缺失",
  original_integrity_error: "原件完整性校验失败",
  managed_source_pending: "新来源尚未完成登记",
};

export function mediaPresentation(bundle: EvidenceBundle, at = Date.now()) {
  const state = bundle.media_lifecycle;
  const readOnly = Boolean(bundle.read_only || state?.read_only);
  const due = state?.delete_due_at ? Date.parse(state.delete_due_at) : NaN;
  const deadlinePast = Boolean(state && !readOnly && Number.isFinite(due) && at >= due);
  const reason = state?.state === "expired" ? "original_expired"
    : state?.operation_state === "purging" ? "original_purge_in_progress"
    : deadlinePast ? "original_expiry_pending" : state?.reason;
  const originalMembers = bundle.media
    .filter((item) => !state || item.bundle_id === state.bundle_id)
    .slice().sort((a, b) => a.index - b.index);
  const originalsAvailable = state
    ? !readOnly && state.http_status === 200 && reason === "original_available"
    : bundle.media_availability.status === "available";
  const previews = (bundle.previews ?? [])
    .filter((item) => item.available !== false && (!state || item.bundle_id === state.bundle_id))
    .slice().sort((a, b) => a.index - b.index);
  const gallery = previews.length ? previews
    : originalsAvailable ? originalMembers.filter((item) => item.available !== false) : [];
  const canRestore = Boolean(state?.can_restore && !readOnly
    && state.http_status === 409 && reason === "original_archived"
    && Number.isFinite(due) && at < due);
  const purgeFailed = reason === "original_purge_in_progress" && Boolean(state?.last_error);
  return {
    reason, readOnly, deadlinePast, originalMembers, originalsAvailable, gallery,
    isPreview: previews.length > 0,
    title: purgeFailed ? "原件删除失败，等待结算" : reason ? titles[reason] ?? "原件暂不可用"
      : bundle.media_availability.status === "available" ? "已保存的媒体" : "暂未提供原件",
    message: deadlinePast && state?.reason !== reason
      ? "已到原定删除时间；不能新增恢复或原件读取，请刷新确认最新结算状态。"
      : bundle.media_availability.reason,
    canRestore, canRestoreForReprocess: canRestore,
    canReprocess: !readOnly && originalsAvailable && (state ? state.can_reprocess : true),
    restoring: !readOnly && reason === "original_restoring",
    canReacquire: false,
  };
}

// The evidence workbench favors small previews; a playback request favors
// originals only after the same source, lifecycle and availability checks.
export function mediaPlaybackPresentation(bundle: EvidenceBundle, at = Date.now()) {
  const presentation = mediaPresentation(bundle, at);
  const originals = presentation.originalsAvailable
    ? presentation.originalMembers.filter((item) => item.available !== false) : [];
  return originals.length
    ? { ...presentation, gallery: originals, isPreview: false }
    : presentation;
}

export function mediaMemberKey(media: EvidenceMedia) {
  return (media.bundle_id ?? "legacy") + ":" + media.artifact_id + ":" + media.index;
}

export function mediaMemberLabel(media: EvidenceMedia, preview = false) {
  return preview
    ? "预览 " + (media.index + 1) + " · 对应原件 " + ((media.original_index ?? media.index) + 1)
    : "原件 " + (media.index + 1);
}

export function mediaBytes(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) return "未知";
  if (value < 1024) return value + " B";
  if (value < 1024 * 1024) return (value / 1024).toFixed(2) + " KiB";
  return value >= 1024 * 1024 * 1024 ? (value / (1024 * 1024 * 1024)).toFixed(2) + " GiB"
    : (value / (1024 * 1024)).toFixed(2) + " MiB";
}

export function mediaAge(seconds: number | null | undefined) {
  if (typeof seconds !== "number" || !Number.isFinite(seconds) || seconds < 0) return "未知";
  return Math.floor(seconds / 86400) + " 天 " + Math.floor(seconds % 86400 / 3600) + " 小时";
}

export function mediaDeadlineText(dueAt: string | null | undefined, at = Date.now()) {
  if (!dueAt) return "尚未开始归档保留计时";
  const due = Date.parse(dueAt);
  if (!Number.isFinite(due)) return "删除期限未知";
  if (at >= due) return "已到原定删除时间；延迟不延长期限";
  const minutes = Math.ceil((due - at) / 60000);
  return "剩余 " + Math.floor(minutes / 1440) + " 天 "
    + Math.floor(minutes % 1440 / 60) + " 小时 " + minutes % 60 + " 分钟";
}

export function mediaBlockerLabel(reason: string | null | undefined) {
  if (!reason) return "原因尚未核实";
  const fixed: Record<string, string> = {
    completion_gate_aged: "完成门未通过满14天",
    protected: "人工保护尚未解除",
    purging: "正在逐件删除",
    purge_failed: "删除失败，等待安全结算",
    restore_in_flight: "恢复任务尚在执行",
    lifecycle_paused: "生命周期处理已暂停",
    awaiting_retention_worker: "等待下一轮删除作业",
    formal_V2_V3_evaluation_missing: "正式媒体评估尚未完成",
    fingerprint_not_persisted: "去重指纹尚未封存",
    preview_members_missing: "保留预览尚未完成",
    original_missing: "原件意外缺失",
    original_integrity_error: "原件完整性校验失败",
  };
  if (fixed[reason]) return fixed[reason];
  if (/asr|transcript/.test(reason)) return "语音转写证据待核验";
  if (/ocr/.test(reason)) return "画面文字识别证据待核验";
  if (/preview|keyframe/.test(reason)) return "预览或关键帧证据待核验";
  if (/fingerprint|duplicate/.test(reason)) return "去重指纹证据待核验";
  if (/evaluation|envelope/.test(reason)) return "评估证据尚未封存";
  if (/source|identity|binding/.test(reason)) return "来源或成员身份待核验";
  if (/sha256|hash|integrity/.test(reason)) return "证据完整性待核验";
  if (/missing|not_available/.test(reason)) return "所需证据尚未齐备";
  return /[A-Za-z_]{3,}|https?:|[\\/]/.test(reason) ? "本地处理异常，待管理员核查" : reason;
}

export function mediaManualPage(items: MediaLifecycleSummary["manual_todos"], page = 1, pageSize = 20) {
  const size = [20, 50, 100].includes(pageSize) ? pageSize : 20;
  const sorted = items.slice().sort((a, b) => {
    const first = Date.parse(a.registered_at ?? "");
    const second = Date.parse(b.registered_at ?? "");
    return (Number.isFinite(first) ? first : Number.MAX_SAFE_INTEGER)
      - (Number.isFinite(second) ? second : Number.MAX_SAFE_INTEGER)
      || a.bundle_id.localeCompare(b.bundle_id);
  });
  const pages = Math.max(1, Math.ceil(sorted.length / size));
  const current = Math.min(pages, Math.max(1, Math.trunc(page) || 1));
  return { items: sorted.slice((current - 1) * size, current * size),
    page: current, pageSize: size, total: sorted.length, pages };
}
