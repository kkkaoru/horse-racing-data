import type { NextConfig } from "next";

import { getCloudflareDevContextOptions, parseAllowedDevOrigins } from "./src/lib/next-config";

const nextConfig: NextConfig = {
  allowedDevOrigins: parseAllowedDevOrigins(process.env.PC_KEIBA_ALLOWED_DEV_ORIGINS),
  serverExternalPackages: ["pg", "pg-cloudflare"],
};

const initCloudflareDevContext = async (): Promise<void> => {
  const mod = await import("@opennextjs/cloudflare");
  await mod.initOpenNextCloudflareForDev(
    getCloudflareDevContextOptions(
      import.meta.url,
      process.env.PC_KEIBA_CLOUDFLARE_REMOTE_BINDINGS,
    ),
  );
};

// Initialize Cloudflare vars for `next dev` via wrangler.dev.jsonc.
// Production Durable Objects are accessed through the production API proxy in local dev.
if (process.env.NODE_ENV === "development") {
  void initCloudflareDevContext().catch((error: unknown) => {
    console.warn("[pc-keiba-viewer] Cloudflare dev context unavailable.", error);
  });
}

export default nextConfig;
