// Run with bun. Cache API wrapper for D1 read queries inside the Worker.

export const SHARED_D1_QUERY_CACHE_NAMESPACE = "horse-racing-data:d1-query:v1";
export const SHARED_D1_QUERY_CACHE_URL_BASE = "https://horse-racing-data.local/d1-query-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";

export type D1QueryCacheProfile =
  | "horse-running-style-history"
  | "realtime-short"
  | "running-style-race"
  | "running-style-races";

export interface D1QueryCacheRaceDayContext {
  kaisaiNen: string;
  kaisaiTsukihi: string;
}

const PROFILE_DEFAULT_TTL_SECONDS: Record<D1QueryCacheProfile, number> = {
  "horse-running-style-history": 60 * 60,
  "realtime-short": 60,
  "running-style-race": 6 * 60 * 60,
  "running-style-races": 6 * 60 * 60,
};

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

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

const getRaceDayExpiresAtMs = (raceDay: D1QueryCacheRaceDayContext): number =>
  Date.parse(
    `${raceDay.kaisaiNen}-${raceDay.kaisaiTsukihi.slice(0, 2)}-${raceDay.kaisaiTsukihi.slice(
      2,
      4,
    )}T23:59:59+09:00`,
  );

const getRaceDayTtlSeconds = (raceDay: D1QueryCacheRaceDayContext, nowMs = Date.now()): number => {
  const expiresAt = getRaceDayExpiresAtMs(raceDay);
  if (!Number.isFinite(expiresAt)) {
    return 0;
  }
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

export const resolveD1QueryCacheTtlSeconds = (
  profile: D1QueryCacheProfile,
  raceDay?: D1QueryCacheRaceDayContext,
  nowMs = Date.now(),
): number => {
  if (
    (profile === "running-style-race" || profile === "running-style-races") &&
    raceDay !== undefined
  ) {
    return getRaceDayTtlSeconds(raceDay, nowMs);
  }
  return PROFILE_DEFAULT_TTL_SECONDS[profile];
};

export const buildD1QueryCacheKey = (
  profile: D1QueryCacheProfile,
  keyParts: readonly unknown[],
): string =>
  hashString(stableStringify([SHARED_D1_QUERY_CACHE_NAMESPACE, profile, keyParts]));

const createCacheRequest = (cacheKey: string): Request =>
  new Request(`${SHARED_D1_QUERY_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

export const readD1QueryCache = async <T>(
  profile: D1QueryCacheProfile,
  keyParts: readonly unknown[],
  options?: {
    raceDay?: D1QueryCacheRaceDayContext;
  },
): Promise<T | null> => {
  const ttlSeconds = resolveD1QueryCacheTtlSeconds(profile, options?.raceDay);
  const cache = getDefaultCache();
  if (ttlSeconds <= 0 || cache === null) {
    return null;
  }

  const cacheKey = buildD1QueryCacheKey(profile, keyParts);
  const cacheRequest = createCacheRequest(cacheKey);
  const cachedResponse = await cache.match(cacheRequest);
  if (cachedResponse) {
    try {
      return (await cachedResponse.json()) as T;
    } catch {
      await cache.delete(cacheRequest);
    }
  }
  return null;
};

export interface PutD1QueryCacheOptions {
  ctx?: ExecutionContext;
  kv?: KVNamespace;
  raceDay?: D1QueryCacheRaceDayContext;
}

const buildResponseForCache = (body: string, ttlSeconds: number): Response =>
  new Response(body, {
    headers: {
      "Cache-Control": `public, max-age=${ttlSeconds}`,
      "Content-Type": DEFAULT_CONTENT_TYPE,
      "X-D1-Query-Cache": "MISS-stored",
    },
  });

export const putD1QueryCache = async <T>(
  profile: D1QueryCacheProfile,
  keyParts: readonly unknown[],
  value: T,
  options?: PutD1QueryCacheOptions,
): Promise<void> => {
  const ttlSeconds = resolveD1QueryCacheTtlSeconds(profile, options?.raceDay);
  if (ttlSeconds <= 0) {
    return;
  }
  const cache = getDefaultCache();
  const cacheKey = buildD1QueryCacheKey(profile, keyParts);
  const body = JSON.stringify(value);
  const writes: Promise<unknown>[] = [];
  if (cache !== null) {
    writes.push(cache.put(createCacheRequest(cacheKey), buildResponseForCache(body, ttlSeconds)));
  }
  if (options?.kv !== undefined) {
    writes.push(options.kv.put(cacheKey, body, { expirationTtl: ttlSeconds }));
  }
  if (writes.length === 0) {
    return;
  }
  const putAll = Promise.all(writes);
  if (options?.ctx !== undefined) {
    options.ctx.waitUntil(putAll);
  } else {
    await putAll;
  }
};

export const withD1QueryCache = async <T>(
  profile: D1QueryCacheProfile,
  keyParts: readonly unknown[],
  load: () => Promise<T>,
  options?: PutD1QueryCacheOptions,
): Promise<T> => {
  const ttlSeconds = resolveD1QueryCacheTtlSeconds(profile, options?.raceDay);
  const cache = getDefaultCache();
  if (ttlSeconds <= 0 || cache === null) {
    return load();
  }

  const cacheKey = buildD1QueryCacheKey(profile, keyParts);
  const cacheRequest = createCacheRequest(cacheKey);
  const cachedResponse = await cache.match(cacheRequest);
  if (cachedResponse) {
    try {
      return (await cachedResponse.json()) as T;
    } catch {
      await cache.delete(cacheRequest);
    }
  }

  const value = await load();
  await putD1QueryCache(profile, keyParts, value, options);
  return value;
};
