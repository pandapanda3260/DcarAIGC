import assert from "node:assert/strict";
import test from "node:test";

import { filterVehicleSeriesGroups, sortVehicleCatalogRows } from "../app/spu-audience/vehicleCatalogSort.ts";

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
