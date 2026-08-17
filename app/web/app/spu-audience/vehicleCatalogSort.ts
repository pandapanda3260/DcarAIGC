// 懂车帝品牌接口的 hot_brand 数组顺序（2026-08-16 快照）。
// 页面只使用本地快照，避免渲染时依赖未公开的远程接口。
const dongchediHotBrands = [
  "奔驰", "宝马", "大众", "奥迪", "小米", "本田", "特斯拉", "蔚来", "吉利", "丰田",
  "比亚迪", "坦克", "路虎", "红旗", "小鹏", "凯迪拉克", "雷克萨斯", "沃尔沃", "五菱", "现代",
] as const;

const brandAliases: Record<string, string> = {
  "小米汽车": "小米",
  "吉利汽车": "吉利",
  "小鹏汽车": "小鹏",
  "五菱汽车": "五菱",
  "理想汽车": "理想",
  "AITO问界": "问界",
};

// 当前目录中未进入热门榜的品牌，按懂车帝 A–Z 分组顺序排列。
// 问界在懂车帝以“AITO问界”归入 A 组；显式 key 也避免“长安”被按 zhang 误排。
const nonHotBrandSortKeys: Record<string, string> = {
  "问界": "aito-wenjie",
  "长安": "changan",
  "哈弗": "hafu",
  "极氪": "jike",
  "理想": "lixiang",
  "日产": "richan",
};

const hotBrandRanks = new Map<string, number>(
  dongchediHotBrands.map((brand, index) => [brand, index]),
);
const pinyinKeyCollator = new Intl.Collator("en", { numeric: true, sensitivity: "base" });
const unknownBrandCollator = new Intl.Collator("zh-CN-u-co-pinyin", { numeric: true, sensitivity: "base" });

function normalizeBrand(brand: string) {
  const trimmed = brand.trim();
  return brandAliases[trimmed] ?? trimmed;
}

function compareBrand(leftBrand: string, rightBrand: string) {
  const left = normalizeBrand(leftBrand);
  const right = normalizeBrand(rightBrand);
  if (left === right) return 0;

  const leftHotRank = hotBrandRanks.get(left);
  const rightHotRank = hotBrandRanks.get(right);
  if (leftHotRank != null || rightHotRank != null) {
    if (leftHotRank == null) return 1;
    if (rightHotRank == null) return -1;
    return leftHotRank - rightHotRank;
  }

  const leftPinyinKey = nonHotBrandSortKeys[left];
  const rightPinyinKey = nonHotBrandSortKeys[right];
  if (leftPinyinKey || rightPinyinKey) {
    // 当前已配置品牌保持稳定；新品牌先安全置后，补充官方分组后即可进入固定位置。
    if (!leftPinyinKey) return 1;
    if (!rightPinyinKey) return -1;
    return pinyinKeyCollator.compare(leftPinyinKey, rightPinyinKey);
  }

  return unknownBrandCollator.compare(left, right);
}

export function sortVehicleCatalogRows<T extends { brand: string }>(rows: readonly T[]): T[] {
  // sort 自 ES2019 起稳定：同品牌返回 0，保留接口已有的车系、兜底节点和款型顺序。
  return [...rows].sort((left, right) => compareBrand(left.brand, right.brand));
}

type VehicleCatalogAlias = {
  alias: string;
  ambiguous: boolean;
};

type VehicleCatalogSearchRow = {
  brand: string;
  series: string;
  trim_label?: string | null;
  model_year?: number | null;
  aliases?: readonly VehicleCatalogAlias[];
};

type VehicleSeriesGroup = {
  seriesNode: VehicleCatalogSearchRow;
  trims?: readonly VehicleCatalogSearchRow[];
};

export type VehicleSeriesMatch = {
  rank: number;
  kind: "brand" | "series" | "series_alias" | "trim";
  matchedTrimLabel?: string;
  matchedTrimCount?: number;
};

type SearchCandidate = {
  value: string;
  compact: string;
  kind: VehicleSeriesMatch["kind"];
  ambiguous: boolean;
  trimLabel?: string;
};

const MATCH_RANK = {
  canonicalExact: 0,
  aliasOrTrimExact: 1,
  canonicalBoundary: 2,
  aliasOrTrimBoundary: 3,
  substringOrMultiTerm: 4,
  ambiguousAlias: 5,
} as const;

function normalizeSearchText(value: string | number | null | undefined) {
  return String(value ?? "")
    .normalize("NFKC")
    .toLocaleLowerCase("zh-CN")
    // `+` 与 `#` 是 P7+、smart 精灵#1 等车型名称的一部分，不能按普通标点丢弃。
    .replace(/[^\p{L}\p{N}+#]+/gu, " ")
    .trim();
}

function compactSearchText(value: string) {
  return value.replace(/\s+/g, "");
}

function isAsciiLetterOrDigit(value: string | undefined) {
  return value != null && /^[a-z0-9]$/i.test(value);
}

function hasAsciiTokenBoundary(value: string, query: string) {
  if (!/^[a-z0-9]+$/i.test(query)) return false;
  let offset = 0;
  while (offset <= value.length - query.length) {
    const index = value.indexOf(query, offset);
    if (index < 0) return false;
    const left = value[index - 1];
    const right = value[index + query.length];
    if (!isAsciiLetterOrDigit(left) && !isAsciiLetterOrDigit(right)) return true;
    offset = index + 1;
  }
  return false;
}

function candidateContainsTerm(candidate: SearchCandidate, term: string) {
  const compactTerm = compactSearchText(term);
  return candidate.value.includes(term) || (compactTerm.length > 0 && candidate.compact.includes(compactTerm));
}

function candidateRank(candidate: SearchCandidate, query: string, compactQuery: string) {
  const exact = candidate.value === query || candidate.compact === compactQuery;
  const prefix = candidate.value.startsWith(query) || candidate.compact.startsWith(compactQuery);
  const boundary = hasAsciiTokenBoundary(candidate.value, query) || hasAsciiTokenBoundary(candidate.compact, compactQuery);
  const substring = candidate.value.includes(query) || candidate.compact.includes(compactQuery);
  if (!exact && !prefix && !boundary && !substring) return null;
  if (candidate.ambiguous) return MATCH_RANK.ambiguousAlias;
  if (candidate.kind === "brand" || candidate.kind === "series") {
    if (exact) return MATCH_RANK.canonicalExact;
    if (prefix || boundary) return MATCH_RANK.canonicalBoundary;
  } else {
    if (exact) return MATCH_RANK.aliasOrTrimExact;
    if (prefix || boundary) return MATCH_RANK.aliasOrTrimBoundary;
  }
  return MATCH_RANK.substringOrMultiTerm;
}

function rankTermsInContext(candidates: readonly SearchCandidate[], terms: readonly string[]) {
  let worstRank: number | null = null;
  for (const term of terms) {
    const compactTerm = compactSearchText(term);
    let bestTermRank: number | null = null;
    for (const candidate of candidates) {
      const rank = candidateRank(candidate, term, compactTerm);
      if (rank != null && (bestTermRank == null || rank < bestTermRank)) bestTermRank = rank;
    }
    if (bestTermRank == null) return null;
    worstRank = worstRank == null ? bestTermRank : Math.max(worstRank, bestTermRank);
  }
  return worstRank;
}

function buildSearchCandidates(group: VehicleSeriesGroup) {
  const candidates: SearchCandidate[] = [];
  const seen = new Set<string>();
  function append(
    rawValue: string | number | null | undefined,
    kind: SearchCandidate["kind"],
    ambiguous = false,
    trimLabel?: string,
  ) {
    const value = normalizeSearchText(rawValue);
    if (!value) return;
    const compact = compactSearchText(value);
    const key = `${kind}\u0000${ambiguous ? 1 : 0}\u0000${trimLabel ?? ""}\u0000${value}`;
    if (seen.has(key)) return;
    seen.add(key);
    candidates.push({ value, compact, kind, ambiguous, trimLabel });
  }

  const { seriesNode } = group;
  append(seriesNode.brand, "brand");
  append(seriesNode.series, "series");
  append(`${seriesNode.brand} ${seriesNode.series}`, "series");
  for (const alias of seriesNode.aliases ?? []) append(alias.alias, "series_alias", alias.ambiguous);
  for (const trim of group.trims ?? []) {
    const trimLabel = trim.trim_label?.trim();
    append(trimLabel, "trim", false, trimLabel || undefined);
    if (trim.model_year != null) {
      append(trim.model_year, "trim", false, trimLabel || undefined);
      append(`${trim.model_year}款`, "trim", false, trimLabel || undefined);
    }
    for (const alias of trim.aliases ?? []) append(alias.alias, "trim", alias.ambiguous, trimLabel || undefined);
  }
  return candidates;
}

export function matchVehicleSeriesGroup(group: VehicleSeriesGroup, rawQuery: string): VehicleSeriesMatch | null {
  const query = normalizeSearchText(rawQuery);
  if (!query) return null;
  const compactQuery = compactSearchText(query);
  const terms = query.split(/\s+/).filter(Boolean);
  const candidates = buildSearchCandidates(group);
  let bestCandidate: SearchCandidate | null = null;
  let bestRank: number | null = null;

  for (const candidate of candidates) {
    const rank = candidateRank(candidate, query, compactQuery);
    if (rank == null || (bestRank != null && rank >= bestRank)) continue;
    bestCandidate = candidate;
    bestRank = rank;
  }

  const nonTrimCandidates = candidates.filter((candidate) => candidate.kind !== "trim");
  const nonTrimRank = rankTermsInContext(nonTrimCandidates, terms);
  const matchingTrimContexts: Array<{ label: string; rank: number }> = [];
  for (const trimLabel of new Set(candidates.map((candidate) => candidate.trimLabel).filter(Boolean) as string[])) {
    const trimCandidates = candidates.filter((candidate) => candidate.trimLabel === trimLabel);
    const rank = rankTermsInContext([...nonTrimCandidates, ...trimCandidates], terms);
    if (rank != null) matchingTrimContexts.push({ label: trimLabel, rank });
  }
  const bestContextRank = [nonTrimRank, ...matchingTrimContexts.map(({ rank }) => rank)]
    .filter((rank): rank is number => rank != null)
    .reduce<number | null>((best, rank) => best == null || rank < best ? rank : best, null);
  if (bestRank == null && bestContextRank == null) return null;
  if (bestContextRank != null && (bestRank == null || bestContextRank < bestRank)) bestRank = bestContextRank;

  const trimRequired = nonTrimRank == null && matchingTrimContexts.length > 0;
  const matchedTrimLabels = new Set<string>();
  if (bestCandidate?.kind === "trim" || trimRequired) {
    for (const { label } of matchingTrimContexts) matchedTrimLabels.add(label);
  }

  const firstMatchingNonTrim = nonTrimCandidates.find((candidate) => (
    terms.some((term) => candidateContainsTerm(candidate, term))
  ));
  const kind = matchedTrimLabels.size > 0 ? "trim" : (bestCandidate?.kind ?? firstMatchingNonTrim?.kind ?? "series");
  if (bestRank == null) return null;
  return {
    rank: bestRank,
    kind,
    ...(matchedTrimLabels.size > 0 ? {
      matchedTrimLabel: matchedTrimLabels.values().next().value,
      matchedTrimCount: matchedTrimLabels.size,
    } : {}),
  };
}

export function filterVehicleSeriesGroups<T extends VehicleSeriesGroup>(
  groups: readonly T[],
  query: string,
): T[] {
  if (!normalizeSearchText(query)) return [...groups];
  return groups
    .map((group, index) => ({ group, index, match: matchVehicleSeriesGroup(group, query) }))
    .filter((entry): entry is typeof entry & { match: VehicleSeriesMatch } => entry.match != null)
    .sort((left, right) => left.match.rank - right.match.rank || left.index - right.index)
    .map(({ group }) => group);
}
