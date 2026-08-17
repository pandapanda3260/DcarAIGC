import assert from "node:assert/strict";
import test from "node:test";

import {
  filterVehicleSeriesGroups,
  matchVehicleSeriesGroup,
  sortVehicleCatalogRows,
} from "../app/spu-audience/vehicleCatalogSort.ts";

function searchRow({ brand, series, aliases = [], trimLabel = null, modelYear = null }) {
  return {
    brand,
    series,
    trim_label: trimLabel,
    model_year: modelYear,
    aliases,
  };
}

function seriesGroup(slug, brand, series, aliases = [], trims = []) {
  return {
    slug,
    seriesNode: searchRow({ brand, series, aliases }),
    trims,
  };
}

function vehicleAlias(alias, ambiguous = false) {
  return { alias, ambiguous };
}

test("vehicle catalog follows Dongchedi hot brands before the remaining A-Z groups", () => {
  const currentBrands = [
    "丰田", "五菱", "吉利", "哈弗", "坦克", "大众", "奔驰", "奥迪", "宝马", "小米",
    "小鹏", "日产", "本田", "极氪", "比亚迪", "特斯拉", "理想", "蔚来", "长安", "问界",
  ];

  assert.deepEqual(
    sortVehicleCatalogRows(currentBrands.map((brand) => ({ brand }))).map((row) => row.brand),
    [
      "奔驰", "宝马", "大众", "奥迪", "小米", "本田", "特斯拉", "蔚来", "吉利", "丰田",
      "比亚迪", "坦克", "小鹏", "五菱", "问界", "长安", "哈弗", "极氪", "理想", "日产",
    ],
  );
});

test("vehicle catalog keeps rows stable inside a brand and does not mutate API data", () => {
  const rows = [
    { brand: "丰田", id: "toyota-second" },
    { brand: "奔驰", id: "benz" },
    { brand: "丰田", id: "toyota-first" },
  ];
  const original = structuredClone(rows);

  assert.deepEqual(
    sortVehicleCatalogRows(rows).map((row) => row.id),
    ["benz", "toyota-second", "toyota-first"],
  );
  assert.deepEqual(rows, original);
});

test("vehicle catalog understands official aliases and sorts unknown brands by pinyin", () => {
  assert.deepEqual(
    sortVehicleCatalogRows([
      { brand: "东风" },
      { brand: "小米汽车" },
      { brand: "阿维塔" },
      { brand: "奔驰" },
    ]).map((row) => row.brand),
    ["奔驰", "小米汽车", "阿维塔", "东风"],
  );
});

test("vehicle catalog search filters brand or series without changing group order", () => {
  const groups = [
    { slug: "benz-c", seriesNode: { brand: "奔驰", series: "奔驰C级" } },
    { slug: "benz-glc", seriesNode: { brand: "奔驰", series: "奔驰GLC" } },
    { slug: "bmw-3", seriesNode: { brand: "宝马", series: "宝马3系" } },
  ];

  assert.deepEqual(filterVehicleSeriesGroups(groups, "奔驰").map((group) => group.slug), ["benz-c", "benz-glc"]);
  assert.deepEqual(filterVehicleSeriesGroups(groups, " glc ").map((group) => group.slug), ["benz-glc"]);
  assert.deepEqual(filterVehicleSeriesGroups(groups, "奔驰 C级").map((group) => group.slug), ["benz-c"]);
  assert.deepEqual(filterVehicleSeriesGroups(groups, "不存在"), []);
  assert.deepEqual(filterVehicleSeriesGroups(groups, "  ").map((group) => group.slug), ["benz-c", "benz-glc", "bmw-3"]);
});

test("vehicle catalog search uses series aliases and NFKC-normalized input", () => {
  const groups = [
    seriesGroup("aito-m8", "AITO问界", "问界M8", [vehicleAlias("赛力斯汽车问界M8")]),
    seriesGroup("aito-m6", "AITO问界", "问界M6", [vehicleAlias("赛力斯汽车问界M6")]),
    seriesGroup("aito-m5", "AITO问界", "问界M5", [vehicleAlias("赛力斯汽车问界M5")]),
    seriesGroup("aito-m7", "问界", "问界M7", [
      vehicleAlias("AITO M7"),
      vehicleAlias("赛力斯汽车问界M7"),
      vehicleAlias("M7", true),
    ]),
    seriesGroup("aito-m9", "问界", "问界M9", [vehicleAlias("AITO M9"), vehicleAlias("M9", true)]),
  ];

  assert.deepEqual(filterVehicleSeriesGroups(groups, "AITO").map((group) => group.slug), [
    "aito-m8", "aito-m6", "aito-m5", "aito-m7", "aito-m9",
  ]);
  assert.deepEqual(filterVehicleSeriesGroups(groups, "ＡＩＴＯ　Ｍ７").map((group) => group.slug), ["aito-m7"]);
  assert.deepEqual(filterVehicleSeriesGroups(groups, "赛力斯 M7").map((group) => group.slug), ["aito-m7"]);
  assert.deepEqual(filterVehicleSeriesGroups(groups, "wenjie"), []);
});

test("vehicle catalog search preserves semantic symbols in model names", () => {
  const groups = [
    seriesGroup("honda-p7", "广汽本田", "广汽本田P7"),
    seriesGroup("xpeng-p7", "小鹏", "小鹏P7"),
    seriesGroup("xpeng-p7-plus", "小鹏", "小鹏P7+"),
    seriesGroup("smart-one", "smart", "smart精灵#1"),
    seriesGroup("generic-one", "示例", "示例1"),
  ];

  assert.deepEqual(filterVehicleSeriesGroups(groups, "P7+").map((group) => group.slug), ["xpeng-p7-plus"]);
  assert.deepEqual(filterVehicleSeriesGroups(groups, "#1").map((group) => group.slug), ["smart-one"]);
});

test("vehicle catalog search returns parent series and trim match metadata", () => {
  const bmw3 = seriesGroup(
    "bmw-3",
    "宝马",
    "宝马3系",
    [vehicleAlias("三系", true), vehicleAlias("325", true)],
    [
      searchRow({
        brand: "宝马",
        series: "宝马3系",
        trimLabel: "2025款 325i M运动套装",
        modelYear: 2025,
        aliases: [vehicleAlias("2025款325i M运动套装"), vehicleAlias("330Li")],
      }),
      searchRow({
        brand: "宝马",
        series: "宝马3系",
        trimLabel: "2024款 325Li M运动套装",
        modelYear: 2024,
        aliases: [vehicleAlias("2024款325Li M运动套装")],
      }),
    ],
  );
  const bmwX3 = seriesGroup("bmw-x3", "宝马", "宝马X3", [], [
    searchRow({ brand: "宝马", series: "宝马X3", trimLabel: "2025款 xDrive30i", modelYear: 2025 }),
  ]);

  assert.deepEqual(filterVehicleSeriesGroups([bmw3, bmwX3], "330Li").map((group) => group.slug), ["bmw-3"]);
  assert.deepEqual(filterVehicleSeriesGroups([bmw3, bmwX3], "宝马 2025款").map((group) => group.slug), ["bmw-3", "bmw-x3"]);
  assert.deepEqual(matchVehicleSeriesGroup(bmw3, "2025款"), {
    rank: 1,
    kind: "trim",
    matchedTrimLabel: "2025款 325i M运动套装",
    matchedTrimCount: 1,
  });

  const specialEditions = seriesGroup("special-editions", "示例", "示例车", [], [
    searchRow({ brand: "示例", series: "示例车", trimLabel: "2025款 50周年纪念版", modelYear: 2025 }),
    searchRow({ brand: "示例", series: "示例车", trimLabel: "2026款 马年版", modelYear: 2026 }),
  ]);
  assert.deepEqual(filterVehicleSeriesGroups([specialEditions], "50周年 2026"), []);
  assert.deepEqual(filterVehicleSeriesGroups([specialEditions], "马年 2025"), []);
});

test("vehicle catalog search ranks real M9 boundaries before EM90 and M90 substrings", () => {
  const groups = [
    seriesGroup("byd-m9", "比亚迪", "比亚迪M9"),
    seriesGroup("volvo-em90", "沃尔沃", "沃尔沃EM90", [], [
      searchRow({ brand: "沃尔沃", series: "沃尔沃EM90", trimLabel: "2025款 智尊版", modelYear: 2025 }),
    ]),
    seriesGroup("aito-m9", "问界", "问界M9", [vehicleAlias("M9", true)], [
      searchRow({ brand: "问界", series: "问界M9", trimLabel: "2025款 Ultra版", modelYear: 2025 }),
    ]),
    seriesGroup("galaxy-m9", "吉利银河", "银河M9"),
    seriesGroup("linghui-m9", "领汇", "领汇M9 DM"),
    seriesGroup("changan-m90", "长安凯程", "睿行M90"),
  ];

  assert.deepEqual(filterVehicleSeriesGroups(groups, "M9").map((group) => group.slug), [
    "byd-m9", "aito-m9", "galaxy-m9", "linghui-m9", "volvo-em90", "changan-m90",
  ]);
  assert.deepEqual(filterVehicleSeriesGroups(groups, "M9 2025").map((group) => group.slug), [
    "aito-m9", "volvo-em90",
  ]);
});

test("vehicle catalog search keeps ambiguous aliases and equal-rank results stable", () => {
  const groups = [
    seriesGroup("first", "甲", "甲一", [vehicleAlias("猎豹", true)]),
    seriesGroup("second", "乙", "乙二", [vehicleAlias("猎豹", true)]),
  ];
  const original = structuredClone(groups);

  assert.deepEqual(filterVehicleSeriesGroups(groups, "猎豹").map((group) => group.slug), ["first", "second"]);
  assert.deepEqual(filterVehicleSeriesGroups(groups, " ").map((group) => group.slug), ["first", "second"]);
  assert.deepEqual(groups, original);
});
