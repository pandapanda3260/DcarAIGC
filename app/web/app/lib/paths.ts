export const WEB_BASE_PATH = process.env.NEXT_PUBLIC_DCAR_BASE_PATH ?? "";

export function publicAssetPath(path: `/${string}`) {
  return `${WEB_BASE_PATH}${path}`;
}
