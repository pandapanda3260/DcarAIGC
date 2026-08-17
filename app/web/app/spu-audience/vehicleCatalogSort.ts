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

export function filterVehicleSeriesGroups<T extends { seriesNode: { brand: string; series: string } }>(
  groups: readonly T[],
  query: string,
): T[] {
  const terms = query.trim().toLocaleLowerCase("zh-CN").split(/\s+/).filter(Boolean);
  if (terms.length === 0) return [...groups];
  return groups.filter(({ seriesNode }) => {
    const searchable = `${seriesNode.brand} ${seriesNode.series}`.toLocaleLowerCase("zh-CN");
    return terms.every((term) => searchable.includes(term));
  });
}
