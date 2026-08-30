import "server-only";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import {
  WIN_RATE_HEATMAP_CACHE_TTL_SECONDS,
  createWinRateHeatmapCacheRequest,
  expandWinRateHeatmapCacheReadKeys,
  isWinRateHeatmapSectionPayload,
  type WinRateHeatmapSectionPayload,
} from "./win-rate-heatmap-cache";

const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";

declare global {
  interface CacheStorage {
    readonly default?: Cache;
  }
}

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const parseHeatmapPayload = (value: unknown): WinRateHeatmapSectionPayload | null =>
  isWinRateHeatmapSectionPayload(value) ? value : null;

const tryParseJson = (text: string): unknown => {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
};

const writeCacheApi = async (
  defaultCache: Cache | null,
  cacheRequest: Request,
  body: string,
): Promise<void> => {
  if (defaultCache === null) {
    return;
  }
  await defaultCache.put(
    cacheRequest,
    new Response(body, {
      headers: {
        "Cache-Control": `public, max-age=${WIN_RATE_HEATMAP_CACHE_TTL_SECONDS}`,
        "Content-Type": DEFAULT_CONTENT_TYPE,
      },
    }),
  );
};

const readCacheApiPayload = async (
  defaultCache: Cache | null,
  cacheRequest: Request,
): Promise<WinRateHeatmapSectionPayload | null> => {
  if (defaultCache === null) {
    return null;
  }
  const cachedResponse = await defaultCache.match(cacheRequest);
  if (!cachedResponse?.ok) {
    return null;
  }
  try {
    const parsed = parseHeatmapPayload(await cachedResponse.json());
    if (parsed !== null) {
      return parsed;
    }
  } catch {
    await defaultCache.delete(cacheRequest);
    return null;
  }
  await defaultCache.delete(cacheRequest);
  return null;
};

const readCachedWinRateHeatmapForKey = async (
  cacheKey: string,
  populateCurrentCacheApi: boolean,
): Promise<WinRateHeatmapSectionPayload | null> => {
  const cacheRequest = createWinRateHeatmapCacheRequest(cacheKey);
  const defaultCache = getDefaultCache();
  const fromCacheApi = await readCacheApiPayload(defaultCache, cacheRequest);
  if (fromCacheApi !== null) {
    return fromCacheApi;
  }

  const { ctx, env } = await safeGetCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) {
    return null;
  }
  const parsed = parseHeatmapPayload(tryParseJson(kvBody));
  if (parsed === null || !populateCurrentCacheApi) {
    return parsed;
  }
  const populateCacheApi = writeCacheApi(defaultCache, cacheRequest, kvBody);
  if (ctx !== null) {
    ctx.waitUntil(populateCacheApi);
  } else {
    await populateCacheApi;
  }
  return parsed;
};

export const getCachedWinRateHeatmapPayload = async (
  cacheKey: string,
): Promise<WinRateHeatmapSectionPayload | null> => {
  const readKeys = expandWinRateHeatmapCacheReadKeys(cacheKey);
  const firstKey = readKeys[0];
  if (firstKey === undefined) {
    return null;
  }
  const currentHit = await readCachedWinRateHeatmapForKey(firstKey, true);
  if (currentHit !== null) {
    return currentHit;
  }
  const fallbackKey = readKeys[1];
  return fallbackKey === undefined ? null : readCachedWinRateHeatmapForKey(fallbackKey, false);
};

export const putWinRateHeatmapCache = async ({
  cacheKey,
  payload,
}: {
  cacheKey: string;
  payload: WinRateHeatmapSectionPayload;
}): Promise<void> => {
  const body = JSON.stringify(payload);
  const cacheRequest = createWinRateHeatmapCacheRequest(cacheKey);
  const defaultCache = getDefaultCache();
  const { env } = await safeGetCloudflareRuntime();
  const kv = env?.DETAIL_SECTION_CACHE_KV;
  if (!kv) {
    throw new Error("DETAIL_SECTION_CACHE_KV is unavailable");
  }
  await kv.put(cacheKey, body, { expirationTtl: WIN_RATE_HEATMAP_CACHE_TTL_SECONDS });
  await writeCacheApi(defaultCache, cacheRequest, body).catch(() => undefined);
};
