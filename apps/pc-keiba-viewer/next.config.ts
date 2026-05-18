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

// Expose Cloudflare bindings (REALTIME_DB, RUNNING_STYLE_MODELS, ...) to
// `next dev` via wrangler's platform proxy. Combined with `"remote": true`
// per binding in wrangler.jsonc, this lets server components read
// production D1 directly during local development. Dynamic + fire-and-forget
// keeps the workerd lib types out of the static type graph and avoids the
// top-level-await restriction in next.config.ts.
if (process.env.NODE_ENV === "development") {
  void import("@opennextjs/cloudflare").then((mod) =>
    mod.initOpenNextCloudflareForDev({ remoteBindings: true }),
  );
}

export default nextConfig;
