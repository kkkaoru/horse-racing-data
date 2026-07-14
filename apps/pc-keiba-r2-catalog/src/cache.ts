import type { CacheStore, KvStore, RaceFeatureFilters, SourceScope } from "./types";

export type CacheDescriptor =
  | { date: string; kind: "race-keys" }
  | {
      date: string;
      keibajoCode?: string;
      kind: "race-features";
      raceBango?: string;
      source: SourceScope;
    };

const CACHE_ORIGIN = "https://pc-keiba-r2-catalog-cache.internal";
const CACHE_VERSION = "v2";

export const cacheRequestFor = (descriptor: CacheDescriptor): Request => {
  const url = new URL(`/${CACHE_VERSION}/${descriptor.kind}`, CACHE_ORIGIN);
  url.searchParams.set("date", descriptor.date);
  if (descriptor.kind === "race-features") {
    url.searchParams.set("source", descriptor.source);
    if (descriptor.keibajoCode) url.searchParams.set("keibajoCode", descriptor.keibajoCode);
    if (descriptor.raceBango) url.searchParams.set("raceBango", descriptor.raceBango);
  }
  return new Request(url);
};

export const kvKeyFor = (descriptor: CacheDescriptor): string => {
  const request = cacheRequestFor(descriptor);
  return `catalog:${CACHE_VERSION}:${request.url.slice(CACHE_ORIGIN.length + 1)}`;
};

export const parsePositiveSeconds = (value: string | undefined, fallback: number): number => {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const readKvRows = async (kv: KvStore, key: string): Promise<string | null> => {
  const value = await kv.get(key);
  if (value === null) return null;
  try {
    const parsed: unknown = JSON.parse(value);
    return isRecord(parsed) && Array.isArray(parsed.rows) ? value : null;
  } catch {
    return null;
  }
};

export const jsonRowsResponse = (body: string, cacheStatus: string): Response =>
  new Response(body, {
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "X-Catalog-Cache": cacheStatus,
    },
  });

export const populateCacheApi = async (
  cache: CacheStore,
  request: Request,
  body: string,
  ttlSeconds: number,
): Promise<void> => {
  const response = new Response(body, {
    headers: {
      "Cache-Control": `public, max-age=${String(ttlSeconds)}`,
      "Content-Type": "application/json; charset=utf-8",
    },
  });
  await cache.put(request, response);
};

export const populateCaches = async (
  cache: CacheStore,
  kv: KvStore,
  descriptor: CacheDescriptor,
  body: string,
  cacheTtlSeconds: number,
  kvTtlSeconds: number,
): Promise<void> => {
  await Promise.allSettled([
    populateCacheApi(cache, cacheRequestFor(descriptor), body, cacheTtlSeconds),
    kv.put(kvKeyFor(descriptor), body, { expirationTtl: kvTtlSeconds }),
  ]);
};

export const purgeDescriptors = async (
  cache: CacheStore,
  kv: KvStore,
  descriptors: CacheDescriptor[],
): Promise<number> => {
  const operations = descriptors.flatMap((descriptor) => [
    cache.delete(cacheRequestFor(descriptor)),
    kv.delete(kvKeyFor(descriptor)),
  ]);
  await Promise.allSettled(operations);
  return descriptors.length;
};

export const featureDescriptor = (filters: RaceFeatureFilters): CacheDescriptor => ({
  date: filters.date,
  keibajoCode: filters.keibajoCode,
  kind: "race-features",
  raceBango: filters.raceBango,
  source: filters.source,
});
