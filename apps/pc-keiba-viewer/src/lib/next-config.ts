import { fileURLToPath } from "node:url";

import type { GetPlatformProxyOptions } from "wrangler";

export const parseAllowedDevOrigins = (value: string | undefined): string[] =>
  value
    ?.split(",")
    .map((origin) => origin.trim())
    .filter((origin) => origin.length > 0) ?? [];

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
