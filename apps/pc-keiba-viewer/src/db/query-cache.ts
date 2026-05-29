import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import { getDatabaseTarget } from "./client";
import { withDbRetry } from "./db-retry";

const DEFAULT_TTL_SECONDS = 60 * 60;
const KV_MAX_TTL_SECONDS = 60 * 60 * 24;
const CACHE_NAMESPACE = "pc-keiba-viewer:db-query:v3";

declare global {
  interface CacheStorage {
    readonly default?: Cache;
  }
}

const getCacheTtlSeconds = (): number => {
  const value = Number(process.env.PC_KEIBA_DB_CACHE_TTL_SECONDS);

  if (!Number.isFinite(value)) {
    return DEFAULT_TTL_SECONDS;
  }

  return Math.max(0, Math.floor(value));
};

const canUseQueryCache = (): boolean => {
  const target = getDatabaseTarget();
  return target === "cloudflare" || target === "neon";
};

const getDefaultCacheOrNull = (): Cache | null =>
  typeof caches !== "undefined" && caches.default ? caches.default : null;

type DetailSectionCacheKv = NonNullable<CloudflareEnv["DETAIL_SECTION_CACHE_KV"]>;

const getDetailSectionCacheKv = async (): Promise<DetailSectionCacheKv | null> => {
  try {
    const context = await getCloudflareContext<Record<string, unknown>, PcKeibaExecutionContext>({
      async: true,
    });
    return context.env?.DETAIL_SECTION_CACHE_KV ?? null;
  } catch {
    return null;
  }
};

const isPlainRecord = (value: unknown): value is Record<string, unknown> =>
  Object.prototype.toString.call(value) === "[object Object]";

const stableStringify = (value: unknown): string =>
  JSON.stringify(value, (_key, item: unknown) => {
    if (!isPlainRecord(item)) {
      return item;
    }

    return Object.fromEntries(
      Object.entries(item).toSorted(([left], [right]) => left.localeCompare(right)),
    );
  });

const hashString = (value: string): string => {
  let hash = 0x811c9dc5;

  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }

  return (hash >>> 0).toString(16).padStart(8, "0");
};

const buildCacheKey = (keyParts: readonly unknown[]): string =>
  hashString(stableStringify([CACHE_NAMESPACE, getDatabaseTarget(), keyParts]));

const buildCacheRequestForKey = (cacheKey: string): Request =>
  new Request(`https://pc-keiba-viewer.local/db-query-cache/${cacheKey}`);

const tryParseJsonUnknown = (text: string): { ok: true; value: unknown } | { ok: false } => {
  try {
    const parsed: unknown = JSON.parse(text);
    return { ok: true, value: parsed };
  } catch {
    return { ok: false };
  }
};

const readFromUrlCacheUnknown = async (
  defaultCache: Cache,
  request: Request,
): Promise<{ found: true; value: unknown } | { found: false }> => {
  const cached = await defaultCache.match(request);
  if (!cached) return { found: false };
  const text = await cached.text();
  const parsed = tryParseJsonUnknown(text);
  if (!parsed.ok) {
    await defaultCache.delete(request);
    return { found: false };
  }
  return { found: true, value: parsed.value };
};

const writeToUrlCache = async (
  defaultCache: Cache,
  request: Request,
  body: string,
  ttlSeconds: number,
): Promise<void> => {
  await defaultCache.put(
    request,
    new Response(body, {
      headers: {
        "Cache-Control": `public, max-age=${ttlSeconds}`,
        "Content-Type": "application/json",
      },
    }),
  );
};

const populateUrlCacheFromKv = async (
  defaultCache: Cache | null,
  request: Request,
  body: string,
  ttlSeconds: number,
): Promise<void> => {
  if (defaultCache === null) return;
  await writeToUrlCache(defaultCache, request, body, Math.min(ttlSeconds, 60));
};

interface CacheLookupContext {
  cacheKey: string;
  defaultCache: Cache | null;
  kv: DetailSectionCacheKv | null;
  request: Request;
  ttlSeconds: number;
}

const loadCachedValue = async (
  context: CacheLookupContext,
): Promise<{ found: true; value: unknown } | { found: false }> => {
  const { cacheKey, defaultCache, kv, request, ttlSeconds } = context;
  if (defaultCache !== null) {
    const fromUrl = await readFromUrlCacheUnknown(defaultCache, request);
    if (fromUrl.found) return fromUrl;
  }
  if (kv === null) return { found: false };
  const kvBody = await kv.get(cacheKey);
  if (kvBody === null || kvBody === "") return { found: false };
  const parsed = tryParseJsonUnknown(kvBody);
  if (!parsed.ok) {
    // Stale or corrupt value — overwrite with short-TTL empty to bypass on next read.
    await kv.put(cacheKey, "", { expirationTtl: 60 });
    return { found: false };
  }
  await populateUrlCacheFromKv(defaultCache, request, kvBody, ttlSeconds);
  return { found: true, value: parsed.value };
};

export const withDbQueryCache = async <T>(
  keyParts: readonly unknown[],
  load: () => Promise<T>,
): Promise<T> => {
  const ttlSeconds = getCacheTtlSeconds();
  const loadWithRetry = (): Promise<T> => withDbRetry(load);

  if (ttlSeconds <= 0 || !canUseQueryCache()) {
    return loadWithRetry();
  }

  const cacheKey = buildCacheKey(keyParts);
  const request = buildCacheRequestForKey(cacheKey);
  const defaultCache = getDefaultCacheOrNull();
  const kv = await getDetailSectionCacheKv();

  const cached = await loadCachedValue({
    cacheKey,
    defaultCache,
    kv,
    request,
    ttlSeconds,
  });
  if (cached.found) {
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion
    return cached.value as T;
  }

  const value = await loadWithRetry();
  const body = JSON.stringify(value);
  const writes: Promise<unknown>[] = [];
  if (defaultCache !== null) {
    writes.push(writeToUrlCache(defaultCache, request, body, ttlSeconds));
  }
  if (kv !== null) {
    writes.push(
      kv.put(cacheKey, body, { expirationTtl: Math.min(ttlSeconds, KV_MAX_TTL_SECONDS) }),
    );
  }
  if (writes.length > 0) {
    await Promise.all(writes);
  }

  return value;
};
