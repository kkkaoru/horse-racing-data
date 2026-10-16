// Run with bun via next dev/build.
import type { NextConfig } from "next";

import { getCloudflareDevContextOptions, resolveAllowedDevOrigins } from "./src/lib/next-config";

const nextConfig: NextConfig = {
  allowedDevOrigins: resolveAllowedDevOrigins(process.env.PC_KEIBA_ALLOWED_DEV_ORIGINS),
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

// Await remote bindings before requests can initialize a context from wrangler.jsonc.
export default async function configureNext(): Promise<NextConfig> {
  if (process.env.NODE_ENV === "development") {
    await initCloudflareDevContext();
  }
  return nextConfig;
}
