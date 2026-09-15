/** A missing relation only means a negative result after current work completes. */
export function duplicateReminder(relationStatus: unknown, originalLink: unknown, legacyFallback = "未发现重复"): string {
  if (typeof originalLink === "string" && originalLink) return `与内容 ${originalLink} 重复`;
  if (relationStatus === "pending") return "查重处理中";
  if (relationStatus === "failed") return "查重失败，需重试";
  return relationStatus === "ready" ? "未发现重复" : legacyFallback;
}
