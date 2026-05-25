import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import {
  RACE_TREND_CACHE_AFTER_START_SECONDS,
  buildRaceTrendCacheKey,
  getRaceTrendCacheTtlSeconds,
  type RaceTrendCacheOptions,
} from "./race-trend-cache";
import type { RaceDetail } from "./race-types";

const CACHE_CONTROL_HEADER = "public, max-age=%d";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/race-trend-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";

type CacheSource = "cache-api" | "kv";

const memoryCache = new Map<string, { body: string; expiresAt: number }>();

const getCloudflareRuntime = async (): Promise<{
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

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getCacheRequest = (cacheKey: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const getConfiguredAfterStartSeconds = (env: CloudflareEnv | null): number => {
  const parsed = Number(env?.PC_KEIBA_RACE_TREND_CACHE_AFTER_START_SECONDS);
  return Number.isFinite(parsed) && parsed >= 60
    ? Math.floor(parsed)
    : RACE_TREND_CACHE_AFTER_START_SECONDS;
};

const buildCachedResponse = (body: string, source: CacheSource | "memory"): Response =>
  new Response(body, {
    headers: {
      "Cache-Control": "public, max-age=60",
      "Content-Type": DEFAULT_CONTENT_TYPE,
      "X-Race-Trend-Cache": `HIT-${source}`,
    },
  });

export const buildRaceTrendCacheKeyForRequest = ({
  options,
}: {
  day: string;
  keibajoCode: string;
  month: string;
  options: RaceTrendCacheOptions;
  raceNumber: string;
  year: string;
}): string => buildRaceTrendCacheKey({ options });

export const getCachedRaceTrendResponse = async (cacheKey: string): Promise<Response | null> => {
  const cachedMemory = memoryCache.get(cacheKey);
  if (cachedMemory) {
    if (cachedMemory.expiresAt > Date.now()) {
      return buildCachedResponse(cachedMemory.body, "memory");
    }
    memoryCache.delete(cacheKey);
  }

  const defaultCache = getDefaultCache();
  const cacheRequest = getCacheRequest(cacheKey);
  const cachedResponse = await defaultCache?.match(cacheRequest);
  if (cachedResponse?.ok) {
    return buildCachedResponse(await cachedResponse.text(), "cache-api");
  }

  const { env, ctx } = await getCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) {
    return null;
  }

  const putCache = async () => {
    await defaultCache?.put(
      cacheRequest,
      new Response(kvBody, {
        headers: {
          "Cache-Control": "public, max-age=60",
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    );
  };
  ctx?.waitUntil(putCache());
  return buildCachedResponse(kvBody, "kv");
};

export const putRaceTrendCache = async ({
  body,
  cacheKey,
  race,
}: {
  body: string;
  cacheKey: string;
  race: RaceDetail;
}): Promise<void> => {
  const { env } = await getCloudflareRuntime();
  const ttlSeconds = getRaceTrendCacheTtlSeconds(race, getConfiguredAfterStartSeconds(env));
  if (ttlSeconds <= 0) {
    return;
  }
  memoryCache.set(cacheKey, {
    body,
    expiresAt: Date.now() + ttlSeconds * 1000,
  });

  const cacheControl = CACHE_CONTROL_HEADER.replace("%d", String(ttlSeconds));
  await Promise.all([
    getDefaultCache()?.put(
      getCacheRequest(cacheKey),
      new Response(body, {
        headers: {
          "Cache-Control": cacheControl,
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: ttlSeconds }),
  ]);
};
