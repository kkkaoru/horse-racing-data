import type { NextConfig } from "next";

const parseAllowedDevOrigins = (value: string | undefined): string[] =>
  value
    ?.split(",")
    .map((origin) => origin.trim())
    .filter((origin) => origin.length > 0) ?? [];

const nextConfig: NextConfig = {
  allowedDevOrigins: parseAllowedDevOrigins(process.env.PC_KEIBA_ALLOWED_DEV_ORIGINS),
  serverExternalPackages: ["pg", "pg-cloudflare"],
};

const initCloudflareDevContext = async (): Promise<void> => {
  const mod = await import("@opennextjs/cloudflare");
  await mod.initOpenNextCloudflareForDev({ remoteBindings: false });
};

// Initialize Cloudflare vars for `next dev` without Wrangler's remote proxy:
// this account cannot create Workers preview sessions, and REALTIME_DB remote
// access is handled explicitly via `wrangler d1 execute --remote`.
if (process.env.NODE_ENV === "development") {
  void initCloudflareDevContext();
}

export default nextConfig;
