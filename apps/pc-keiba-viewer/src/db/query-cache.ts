import "server-only";
import { getDatabaseTarget } from "./client";
import { withDbRetry } from "./db-retry";

const DEFAULT_TTL_SECONDS = 60 * 60;
const CACHE_NAMESPACE = "pc-keiba-viewer:db-query:v1";

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
  return (
    (target === "cloudflare" || target === "neon") &&
    typeof caches !== "undefined" &&
    Boolean(caches.default)
  );
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

const createCacheRequest = (keyParts: readonly unknown[]): Request => {
  const cacheKey = stableStringify([CACHE_NAMESPACE, getDatabaseTarget(), keyParts]);
  return new Request(`https://pc-keiba-viewer.local/db-query-cache/${hashString(cacheKey)}`);
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

  const request = createCacheRequest(keyParts);
  const defaultCache = caches.default;

  if (!defaultCache) {
    return loadWithRetry();
  }

  const cached = await defaultCache.match(request);

  if (cached) {
    try {
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion
      return (await cached.json()) as T;
    } catch {
      await defaultCache.delete(request);
    }
  }

  const value = await loadWithRetry();
  await defaultCache.put(
    request,
    new Response(JSON.stringify(value), {
      headers: {
        "Cache-Control": `public, max-age=${ttlSeconds}`,
        "Content-Type": "application/json",
      },
    }),
  );

  return value;
};
