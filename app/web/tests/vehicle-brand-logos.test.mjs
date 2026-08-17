import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

const expectedLogos = {
  "丰田": "toyota.png", "五菱": "wuling.png", "吉利": "geely.png", "哈弗": "haval.png",
  "坦克": "tank.png", "大众": "volkswagen.png", "奔驰": "mercedes-benz.png", "奥迪": "audi.png",
  "宝马": "bmw.png", "小米": "xiaomi.png", "小鹏": "xpeng.png", "日产": "nissan.png",
  "本田": "honda.png", "极氪": "zeekr.png", "比亚迪": "byd.png", "特斯拉": "tesla.png",
  "理想": "li-auto.png", "蔚来": "nio.png", "长安": "changan.png", "问界": "aito.png",
};

test("vehicle catalog bundles a local logo for every current brand", async () => {
  const mapping = await readFile(new URL("../app/spu-audience/vehicleBrandLogos.ts", import.meta.url), "utf8");
  for (const [brand, fileName] of Object.entries(expectedLogos)) {
    assert.match(mapping, new RegExp(`"${brand}": "${fileName.replace(".", "\\.")}"`));
  }
  assert.match(mapping, /return fileName \? publicAssetPath\(`\/vehicle-brand-logos\/\$\{fileName\}`\) : null/);

  await Promise.all(Object.values(expectedLogos).map(async (fileName) => {
    const asset = new URL(`../public/vehicle-brand-logos/${fileName}`, import.meta.url);
    await access(asset);
    const bytes = await readFile(asset);
    assert.ok(bytes.length > 500, `${fileName} should not be an empty placeholder`);
    assert.deepEqual([...bytes.subarray(0, 8)], [137, 80, 78, 71, 13, 10, 26, 10]);
  }));
});

test("vehicle catalog renders logos beside the brand and keeps a failure fallback", async () => {
  const [page, logo, styles] = await Promise.all([
    readFile(new URL("../app/spu-audience/SpuAudiencePage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/spu-audience/VehicleBrandLogo.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);

  assert.match(page, /<VehicleBrandLogo brand=\{seriesNode\.brand\} \/>/);
  assert.match(page, /className="spu-brand-name">\{seriesNode\.brand\}/);
  assert.match(logo, /failedSource === source/);
  assert.match(logo, /<CarIcon size=\{16\}/);
  assert.match(logo, /alt=""/);
  assert.match(styles, /\.spu-brand-cell\s*\{[^}]*grid-template-columns:\s*28px minmax\(0, 1fr\)/);
  assert.match(styles, /\.vehicle-brand-logo-image\s*\{[^}]*object-fit:\s*contain/);
});
