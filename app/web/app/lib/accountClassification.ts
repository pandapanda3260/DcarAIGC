export const accountGroupOptions = [
  { value: "unknown", label: "未填写" },
  { value: "mixed_edit", label: "混剪号" },
  { value: "innovation", label: "创新号" },
  { value: "image_text", label: "图文号" },
  { value: "boutique_ip", label: "精品IP号" },
] as const;

export const businessDirectionOptions = [
  { value: "unknown", label: "未填写" },
  { value: "new_car", label: "新车" },
  { value: "used_car_c1", label: "二手车C1" },
  { value: "used_car_c2", label: "二手车C2" },
  { value: "ai_xiaodong", label: "AI小懂" },
] as const;

export type AccountGroup = typeof accountGroupOptions[number]["value"];
export type BusinessDirection = typeof businessDirectionOptions[number]["value"];

export function accountGroupLabel(value: string | null | undefined) {
  return accountGroupOptions.find((option) => option.value === value)?.label ?? "未填写";
}

export function businessDirectionLabel(value: string | null | undefined) {
  return businessDirectionOptions.find((option) => option.value === value)?.label ?? "未填写";
}
