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

  const { ctx, env } = await safeGetCloudflareRuntime();
  const defaultCache = getDefaultCache();
  const cacheRequest = createPredictionKvCacheRequest(cacheKey);

  // Today's prediction is overwritten after the post-weight rescore. A
  // colo-local Cache API entry may still contain the pre-weight payload even
  // after the global KV value has propagated, so reading Cache API first can
  // display an old generation for the cache TTL. Read KV first for today's
  // races and refresh the local cache from that authoritative value.
  if (window === "today") {
    const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
    if (kvBody) {
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
    }
  }

  const cachedResponse = await defaultCache?.match(cacheRequest);
  if (cachedResponse?.ok) {
    const text = await cachedResponse.text();
    if (text.length > 0) return text;
    await defaultCache?.delete(cacheRequest);
  }

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
  awaitWrite,
  body,
  cacheKey,
  nowMs = Date.now(),
  raceYmd,
}: {
  awaitWrite?: boolean;
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
  if (ctx !== null && awaitWrite !== true) {
    ctx.waitUntil(putCaches);
    return;
  }
  await putCaches;
};

export const deletePredictionKvText = async (cacheKey: string): Promise<void> => {
  const { env } = await safeGetCloudflareRuntime();
  await Promise.all([
    deletePredictionCacheApiCopy(cacheKey),
    env?.DETAIL_SECTION_CACHE_KV?.delete(cacheKey).catch(swallowCacheRejection),
  ]);
};

// Cache-API-tier-only delete. The KV tier holds the fresh value after a
// producer overwrite, so a rescore notification must purge only the colo
// Cache API copy -- deleting KV here would remove the just-written score and
// force a fallback to the stale Neon source.
export const deletePredictionCacheApiCopy = async (cacheKey: string): Promise<void> => {
  await getDefaultCache()
    ?.delete(createPredictionKvCacheRequest(cacheKey))
    .catch(swallowCacheRejection);
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

// Purges only the Cache API tier for both prediction keys of a race. Safe to
// call before OR after a producer writes the KV tier.
export const bustPredictionCacheApiForRace = async ({
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
  await Promise.all(keys.map((key) => deletePredictionCacheApiCopy(key)));
  return { busted: keys.length };
};
