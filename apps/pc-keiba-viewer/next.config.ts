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
  await mod.initOpenNextCloudflareForDev({
    configPath: "./wrangler.dev.jsonc",
    remoteBindings: false,
  });
};

// Initialize Cloudflare vars for `next dev` via wrangler.dev.jsonc.
// Production Durable Objects are accessed through the production API proxy in local dev.
if (process.env.NODE_ENV === "development") {
  void initCloudflareDevContext();
}

export default nextConfig;
