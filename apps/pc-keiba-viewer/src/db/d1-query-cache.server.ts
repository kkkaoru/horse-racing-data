import "server-only";
import { safeGetCloudflareRuntime } from "../lib/cloudflare-context.server";
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

const canUseD1QueryCache = (): boolean => typeof caches !== "undefined" && Boolean(caches.default);

const tryParseJsonUnknown = (text: string): { ok: true; value: unknown } | { ok: false } => {
  try {
    const parsed: unknown = JSON.parse(text);
    return { ok: true, value: parsed };
  } catch {
    return { ok: false };
  }
};

const tryReadCachedJsonUnknown = async (
  response: Response,
): Promise<{ ok: true; value: unknown } | { ok: false }> => {
  try {
    const parsed: unknown = await response.json();
    return { ok: true, value: parsed };
  } catch {
    return { ok: false };
  }
};

interface ReadD1CacheContext {
  cacheKey: string;
  cacheRequest: Request;
  ctx: PcKeibaExecutionContext | null;
  defaultCache: Cache | undefined;
  env: CloudflareEnv | null;
  ttlSeconds: number;
}

const readFromCfCacheUnknown = async (
  context: ReadD1CacheContext,
): Promise<{ found: true; value: unknown } | { found: false }> => {
  const { cacheRequest, defaultCache } = context;
  const cachedResponse = await defaultCache?.match(cacheRequest);
  if (!cachedResponse) return { found: false };
  const parsed = await tryReadCachedJsonUnknown(cachedResponse);
  if (!parsed.ok) {
    await defaultCache?.delete(cacheRequest);
    return { found: false };
  }
  return { found: true, value: parsed.value };
};

const readFromKvUnknown = async (
  context: ReadD1CacheContext,
): Promise<{ found: true; value: unknown } | { found: false }> => {
  const { cacheKey, cacheRequest, ctx, defaultCache, env, ttlSeconds } = context;
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) return { found: false };
  const parsed = tryParseJsonUnknown(kvBody);
  if (!parsed.ok) {
    await env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, "", { expirationTtl: 1 });
    return { found: false };
  }
  const putCache = async (): Promise<void> => {
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
  return { found: true, value: parsed.value };
};

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
  const { ctx, env } = await safeGetCloudflareRuntime();
  const context: ReadD1CacheContext = {
    cacheKey,
    cacheRequest,
    ctx,
    defaultCache,
    env,
    ttlSeconds,
  };
  const fromCache = await readFromCfCacheUnknown(context);
  if (fromCache.found) {
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion
    return fromCache.value as T;
  }
  const fromKv = await readFromKvUnknown(context);
  if (fromKv.found) {
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion
    return fromKv.value as T;
  }
  return null;
};

export const writeD1QueryCache = async (
  profile: D1QueryCacheProfile,
  keyParts: readonly unknown[],
  value: unknown,
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
  const { ctx, env } = await safeGetCloudflareRuntime();
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
