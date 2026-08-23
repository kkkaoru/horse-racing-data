import { fileURLToPath } from "node:url";

import type { GetPlatformProxyOptions } from "wrangler";

export const DEFAULT_ALLOWED_DEV_ORIGINS = [
  "localhost",
  "127.0.0.1",
  "192.168.1.219",
  "localhost:3000",
  "127.0.0.1:3000",
  "localhost:443",
  "127.0.0.1:443",
  "192.168.1.219:443",
] satisfies readonly string[];

export const parseAllowedDevOrigins = (value: string | undefined): string[] =>
  value
    ?.split(",")
    .map((origin) => origin.trim())
    .filter((origin) => origin.length > 0) ?? [];

export const resolveAllowedDevOrigins = (value: string | undefined): string[] => [
  ...new Set([...DEFAULT_ALLOWED_DEV_ORIGINS, ...parseAllowedDevOrigins(value)]),
];

export const shouldEnableCloudflareRemoteBindings = (value: string | undefined): boolean =>
  value === "1";

export const getCloudflareDevConfigPath = (nextConfigUrl: string): string =>
  fileURLToPath(new URL("./wrangler.dev.jsonc", nextConfigUrl));

export const getCloudflareDevContextOptions = (
  nextConfigUrl: string,
  remoteBindingsValue: string | undefined,
): GetPlatformProxyOptions => ({
  configPath: getCloudflareDevConfigPath(nextConfigUrl),
  remoteBindings: shouldEnableCloudflareRemoteBindings(remoteBindingsValue),
});
