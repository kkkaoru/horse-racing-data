// Run with bun. Verify downstream caches against the same Catalog dependency as aggregates.
import "server-only";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
const REVISION: RegExp = /^gate-v1-[a-f0-9]{64}$/u;
export const parseGateMappingRevision = (value: unknown): string => {
  if (typeof value !== "string" || !REVISION.test(value))
    throw new Error("Gate mapping provenance is unavailable");
  return value;
};
export const getCurrentGateMappingRevision = async (
  cacheKey: string,
): Promise<string | undefined> => {
  const race = /:(\d{4}):(\d{1,2}):(\d{1,2}):([A-Z][0-9A-Z]):(\d{2}):/u.exec(cacheKey);
  if (
    race === null ||
    (!cacheKey.startsWith("pc-keiba-viewer:win-rate-heatmap:") && !cacheKey.includes(":condition:"))
  )
    return undefined;

  const env = await safeGetCloudflareEnv();
  if (!env?.R2_CATALOG) throw new Error("Catalog binding unavailable");
  const date: string = race[1]! + race[2]!.padStart(2, "0") + race[3]!.padStart(2, "0");
  if (
    !/^[1-9][0-9]{3}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])$/u.test(date) ||
    new Date(date.slice(0, 4) + "-" + date.slice(4, 6) + "-" + date.slice(6, 8))
      .toISOString()
      .slice(0, 10)
      .replaceAll("-", "") !== date
  )
    throw new Error("Invalid revision scope");
  if (typeof env.R2_CATALOG.getGateMappingRevision !== "function")
    throw new Error("Catalog revision RPC unavailable");
  return parseGateMappingRevision(
    await env.R2_CATALOG.getGateMappingRevision({ date, keibajoCode: race[4]! }),
  );
};
export const hasCurrentGateMappingRevision = async (
  cacheKey: string,
  value: unknown,
): Promise<boolean> => {
  try {
    const current = await getCurrentGateMappingRevision(cacheKey);
    return current === undefined || current === parseGateMappingRevision(value);
  } catch {
    return false;
  }
};

export const hasCurrentGateMappingBody = async (
  cacheKey: string,
  body: string,
): Promise<boolean> => {
  if (!/:\d{4}:\d{1,2}:\d{1,2}:[A-Z][0-9A-Z]:\d{2}:condition:/u.test(cacheKey)) return true;
  try {
    const value: unknown = JSON.parse(body);
    return await hasCurrentGateMappingRevision(
      cacheKey,
      typeof value === "object" && value !== null
        ? Reflect.get(value, "mappingRevision")
        : undefined,
    );
  } catch {
    return false;
  }
};
