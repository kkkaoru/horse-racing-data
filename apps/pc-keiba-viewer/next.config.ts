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

export default nextConfig;
