import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import {
  buildD1QueryCacheKey,
  createD1QueryCacheRequest,
  type D1QueryCacheProfile,
  type D1QueryCacheRaceDayContext,
  resolveD1QueryCacheTtlSeconds,
} from "../lib/d1-query-cache";

export {
  buildD1QueryCacheKey,
  resolveD1QueryCacheTtlSeconds,
  SHARED_D1_QUERY_CACHE_NAMESPACE,
  SHARED_D1_QUERY_CACHE_URL_BASE,
  type D1QueryCacheProfile,
  type D1QueryCacheRaceDayContext,
} from "../lib/d1-query-cache";

const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";

declare global {
  interface CacheStorage {
    readonly default?: Cache;
  }
}

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

const canUseD1QueryCache = (): boolean =>
  typeof caches !== "undefined" && Boolean(caches.default);

export const readD1QueryCache = async <T>(
  profile: D1QueryCacheProfile,
  keyParts: readonly unknown[],
  options?: {
    raceDay?: D1QueryCacheRaceDayContext;
  },
): Promise<T | null> => {
  const ttlSeconds = resolveD1QueryCacheTtlSeconds(profile, options?.raceDay);
  if (ttlSeconds <= 0 || !canUseD1QueryCache()) {
    return null;
  }

  const cacheKey = buildD1QueryCacheKey(profile, keyParts);
  const cacheRequest = createD1QueryCacheRequest(cacheKey);
  const defaultCache = caches.default;
  const { ctx, env } = await getCloudflareRuntime();

  const cachedResponse = await defaultCache?.match(cacheRequest);
  if (cachedResponse) {
    try {
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion
      return (await cachedResponse.json()) as T;
    } catch {
      await defaultCache?.delete(cacheRequest);
    }
  }

  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) {
    return null;
  }

  try {
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion
    const parsed = JSON.parse(kvBody) as T;
    const putCache = async () => {
      await defaultCache?.put(
        cacheRequest,
        new Response(kvBody, {
          headers: {
            "Cache-Control": `public, max-age=${Math.min(ttlSeconds, 60)}`,
            "Content-Type": DEFAULT_CONTENT_TYPE,
            "X-D1-Query-Cache": "HIT-kv",
          },
        }),
      );
    };
    if (ctx !== null) {
      ctx.waitUntil(putCache());
    } else {
      await putCache();
    }
    return parsed;
  } catch {
    await env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, "", { expirationTtl: 1 });
    return null;
  }
};

export const writeD1QueryCache = async <T>(
  profile: D1QueryCacheProfile,
  keyParts: readonly unknown[],
  value: T,
  options?: {
    raceDay?: D1QueryCacheRaceDayContext;
  },
): Promise<void> => {
  const ttlSeconds = resolveD1QueryCacheTtlSeconds(profile, options?.raceDay);
  if (ttlSeconds <= 0 || !canUseD1QueryCache()) {
    return;
  }

  const cacheKey = buildD1QueryCacheKey(profile, keyParts);
  const cacheRequest = createD1QueryCacheRequest(cacheKey);
  const defaultCache = caches.default;
  const { ctx, env } = await getCloudflareRuntime();
  const body = JSON.stringify(value);
  const putCaches = async () => {
    await Promise.all([
      defaultCache?.put(
        cacheRequest,
        new Response(body, {
          headers: {
            "Cache-Control": `public, max-age=${Math.min(ttlSeconds, 60)}`,
            "Content-Type": DEFAULT_CONTENT_TYPE,
            "X-D1-Query-Cache": "MISS-stored",
          },
        }),
      ),
      env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: ttlSeconds }),
    ]);
  };
  if (ctx !== null) {
    ctx.waitUntil(putCaches());
  } else {
    await putCaches();
  }
};
