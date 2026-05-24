// KV/Cache-API backed store for the per-race historical-results payload
// served by `/api/races/.../recent-results`. Extracted so the cache-warm
// cron can populate the cache for tomorrow's races without going through an
// HTTP self-call, which would block on Cloudflare Access in production.
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import type { RaceSource } from "./codes";

const CACHE_NAMESPACE = "pc-keiba-viewer:recent-results:v1";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/recent-results-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";
const DEFAULT_CACHE_TTL_SECONDS = 6 * 60 * 60;
const MIN_KV_TTL_SECONDS = 60;

export const RECENT_RESULTS_CACHE_TTL_SECONDS = DEFAULT_CACHE_TTL_SECONDS;

export type RecentResultsSourceScope = RaceSource | "all";

export interface RecentResultsCacheKeyParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  sourceScope: RecentResultsSourceScope;
  year: string;
}

export const buildRecentResultsCacheKey = (params: RecentResultsCacheKeyParams): string =>
  [
    CACHE_NAMESPACE,
    params.source,
    params.year,
    params.month,
    params.day,
    params.keibajoCode,
    params.raceNumber,
    params.sourceScope,
  ].join(":");

const getCacheRequest = (cacheKey: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const tryGetCloudflareRuntime = async (): Promise<{
  ctx: PcKeibaExecutionContext | null;
  env: CloudflareEnv | null;
}> => {
  try {
    const context = await getCloudflareContext<Record<string, unknown>, PcKeibaExecutionContext>({
      async: true,
    });
    return { ctx: context.ctx, env: context.env };
  } catch {
    return { ctx: null, env: null };
  }
};

const promoteKvBodyToCacheApi = async (body: string, cacheKey: string): Promise<void> => {
  const cache = getDefaultCache();
  if (!cache) {
    return;
  }
  await cache.put(
    getCacheRequest(cacheKey),
    new Response(body, {
      headers: {
        "Cache-Control": "public, max-age=60",
        "Content-Type": DEFAULT_CONTENT_TYPE,
      },
    }),
  );
};

export const getCachedRecentResultsBody = async (cacheKey: string): Promise<string | null> => {
  const cache = getDefaultCache();
  const cachedResponse = await cache?.match(getCacheRequest(cacheKey));
  if (cachedResponse?.ok) {
    return cachedResponse.text();
  }
  const { env, ctx } = await tryGetCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) {
    return null;
  }
  if (cache) {
    ctx?.waitUntil(promoteKvBodyToCacheApi(kvBody, cacheKey));
  }
  return kvBody;
};

export const putRecentResultsCache = async (cacheKey: string, body: string): Promise<void> => {
  const cache = getDefaultCache();
  const { env } = await tryGetCloudflareRuntime();
  await Promise.all([
    cache?.put(
      getCacheRequest(cacheKey),
      new Response(body, {
        headers: {
          "Cache-Control": `public, max-age=${DEFAULT_CACHE_TTL_SECONDS}`,
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, {
      expirationTtl: Math.max(MIN_KV_TTL_SECONDS, DEFAULT_CACHE_TTL_SECONDS),
    }),
  ]);
};
