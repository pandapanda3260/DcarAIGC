import { publicAssetPath } from "../lib/paths";

// The catalog currently contains this bounded brand set. Keep the mapping
// explicit so an unknown brand degrades safely instead of requesting a
// guessed or third-party URL at render time.
export const vehicleBrandLogoFiles = {
  "丰田": "toyota.png",
  "五菱": "wuling.png",
  "吉利": "geely.png",
  "哈弗": "haval.png",
  "坦克": "tank.png",
  "大众": "volkswagen.png",
  "奔驰": "mercedes-benz.png",
  "奥迪": "audi.png",
  "宝马": "bmw.png",
  "小米": "xiaomi.png",
  "小鹏": "xpeng.png",
  "日产": "nissan.png",
  "本田": "honda.png",
  "极氪": "zeekr.png",
  "比亚迪": "byd.png",
  "特斯拉": "tesla.png",
  "理想": "li-auto.png",
  "蔚来": "nio.png",
  "长安": "changan.png",
  "问界": "aito.png",
} as const;

export function vehicleBrandLogoPath(brand: string) {
  const fileName = (vehicleBrandLogoFiles as Record<string, string>)[brand.trim()];
  return fileName ? publicAssetPath(`/vehicle-brand-logos/${fileName}`) : null;
}
