// Run with bun. Shared DETAIL_SECTION_CACHE_KV + Cache API accessors for
// prediction payloads (finish-position and running-style). Window / TTL
// policy lives in prediction-kv-cache.ts; this file only talks to bindings.
import "server-only";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import {
  createPredictionKvCacheRequest,
  getPredictionCacheApiTtlSeconds,
  getPredictionKvTtlSeconds,
  resolvePredictionCacheWindow,
} from "./prediction-kv-cache";

const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const swallowCacheRejection = (): undefined => undefined;

export const readPredictionKvText = async (
  cacheKey: string,
  raceYmd: string,
  nowMs = Date.now(),
): Promise<string | null> => {
  const window = resolvePredictionCacheWindow(raceYmd, nowMs);
  if (window === "outside") return null;

  const defaultCache = getDefaultCache();
  const cacheRequest = createPredictionKvCacheRequest(cacheKey);
  const cachedResponse = await defaultCache?.match(cacheRequest);
  if (cachedResponse?.ok) {
    const text = await cachedResponse.text();
    if (text.length > 0) return text;
    await defaultCache?.delete(cacheRequest);
  }

  const { ctx, env } = await safeGetCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) return null;

  const cacheApiTtl = getPredictionCacheApiTtlSeconds(window);
  const putCache = async (): Promise<void> => {
    await defaultCache?.put(
      cacheRequest,
      new Response(kvBody, {
        headers: {
          "Cache-Control": `public, max-age=${cacheApiTtl}`,
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    );
  };
  if (ctx !== null) {
    ctx.waitUntil(putCache());
  } else {
    await putCache();
  }
  return kvBody;
};

export const writePredictionKvText = async ({
  body,
  cacheKey,
  nowMs = Date.now(),
  raceYmd,
}: {
  body: string;
  cacheKey: string;
  nowMs?: number;
  raceYmd: string;
}): Promise<void> => {
  const window = resolvePredictionCacheWindow(raceYmd, nowMs);
  const kvTtl = getPredictionKvTtlSeconds(window);
  const cacheApiTtl = getPredictionCacheApiTtlSeconds(window);
  if (kvTtl <= 0 || body.length === 0) return;

  const { ctx, env } = await safeGetCloudflareRuntime();
  const defaultCache = getDefaultCache();
  const cacheRequest = createPredictionKvCacheRequest(cacheKey);
  const putCaches = Promise.all([
    defaultCache?.put(
      cacheRequest,
      new Response(body, {
        headers: {
          "Cache-Control": `public, max-age=${cacheApiTtl}`,
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: kvTtl }),
  ]);
  if (ctx !== null) {
    ctx.waitUntil(putCaches);
    return;
  }
  await putCaches;
};

export const deletePredictionKvText = async (cacheKey: string): Promise<void> => {
  const defaultCache = getDefaultCache();
  const { env } = await safeGetCloudflareRuntime();
  await Promise.all([
    defaultCache?.delete(createPredictionKvCacheRequest(cacheKey)).catch(swallowCacheRejection),
    env?.DETAIL_SECTION_CACHE_KV?.delete(cacheKey).catch(swallowCacheRejection),
  ]);
};

export const bustPredictionCachesForRace = async ({
  keibajoCode,
  mmdd,
  raceBango,
  source,
  year,
}: {
  keibajoCode: string;
  mmdd: string;
  raceBango: string;
  source: "jra" | "nar";
  year: string;
}): Promise<{ busted: number }> => {
  const { buildPredictionCacheBustKeys } = await import("./prediction-kv-cache");
  const keys = buildPredictionCacheBustKeys({
    keibajoCode,
    mmdd,
    raceBango,
    source,
    year,
  });
  await Promise.all(keys.map((key) => deletePredictionKvText(key)));
  return { busted: keys.length };
};
