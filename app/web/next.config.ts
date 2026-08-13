import type { NextConfig } from "next";

const configuredBasePath = (process.env.DCAR_WEB_BASE_PATH ?? "").trim();
if (
  configuredBasePath
  && (!configuredBasePath.startsWith("/") || configuredBasePath.endsWith("/"))
) {
  throw new Error("DCAR_WEB_BASE_PATH must start with '/' and must not end with '/'");
}
const nextConfig: NextConfig = {
  basePath: configuredBasePath,
  env: {
    NEXT_PUBLIC_DCAR_BASE_PATH: configuredBasePath,
  },
};

export default nextConfig;
