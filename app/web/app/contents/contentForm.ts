export type ContentForm = {
  id: number | null;
  platform: string;
  platformContentId: string;
  canonicalUrl: string;
  publishedAt: string;
  title: string;
  body: string;
  contentType: string;
  accountUid: string;
  accountName: string;
  accountType: string;
  contentDirection: string;
};

export const emptyContentForm: ContentForm = {
  id: null,
  platform: "douyin",
  platformContentId: "",
  canonicalUrl: "",
  publishedAt: "",
  title: "",
  body: "",
  contentType: "unknown",
  accountUid: "",
  accountName: "",
  accountType: "unknown",
  contentDirection: "unknown",
};

const formFields = [
  ["platform", "platform"],
  ["platformContentId", "platform_content_id"],
  ["canonicalUrl", "canonical_url"],
  ["publishedAt", "published_at"],
  ["title", "title"],
  ["body", "body"],
  ["contentType", "content_type"],
  ["accountUid", "account_uid"],
  ["accountName", "account_name"],
  ["accountType", "account_type"],
  ["contentDirection", "content_direction"],
] as const satisfies ReadonlyArray<readonly [keyof ContentForm, string]>;

function datePart(parts: Intl.DateTimeFormatPart[], type: Intl.DateTimeFormatPartTypes) {
  return parts.find((part) => part.type === type)?.value ?? "";
}

export function toShanghaiDateTimeLocal(value: string | null | undefined): string {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(date);
  return `${datePart(parts, "year")}-${datePart(parts, "month")}-${datePart(parts, "day")}T${datePart(parts, "hour")}:${datePart(parts, "minute")}`;
}

export function fromShanghaiDateTimeLocal(value: string): string | null {
  if (!value) return null;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/.exec(value);
  if (!match) throw new Error("发布日期格式无效");
  const [, year, month, day, hour, minute] = match;
  const utc = Date.UTC(
    Number(year),
    Number(month) - 1,
    Number(day),
    Number(hour) - 8,
    Number(minute),
  );
  const result = new Date(utc);
  if (
    Number.isNaN(result.getTime()) ||
    toShanghaiDateTimeLocal(result.toISOString()) !== value
  ) {
    throw new Error("发布日期格式无效");
  }
  return result.toISOString();
}

function serializeField(field: keyof ContentForm, value: ContentForm[keyof ContentForm]) {
  if (field === "platformContentId") return value || null;
  if (field === "publishedAt") return fromShanghaiDateTimeLocal(String(value));
  return value;
}

export function buildContentRequest(form: ContentForm): Record<string, unknown> {
  return Object.fromEntries(
    formFields.map(([field, requestField]) => [
      requestField,
      serializeField(field, form[field]),
    ]),
  );
}

export function buildContentPatch(
  original: ContentForm,
  current: ContentForm,
): Record<string, unknown> {
  return Object.fromEntries(
    formFields
      .filter(([field]) => current[field] !== original[field])
      .map(([field, requestField]) => [
        requestField,
        serializeField(field, current[field]),
      ]),
  );
}

export function buildContentSaveOperation(
  form: ContentForm,
  original: ContentForm | null,
): {
  path: string;
  method: "POST" | "PATCH";
  body: Record<string, unknown>;
  unchanged: boolean;
} {
  if (form.id === null) {
    return {
      path: "/api/v8/contents",
      method: "POST",
      body: buildContentRequest(form),
      unchanged: false,
    };
  }
  if (original === null) {
    throw new Error("无法保存：没有找到修改前的数据，请刷新页面后重试。");
  }
  const body = buildContentPatch(original, form);
  return {
    path: `/api/v8/contents/${form.id}`,
    method: "PATCH",
    body,
    unchanged: Object.keys(body).length === 0,
  };
}
