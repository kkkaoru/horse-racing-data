import type {
  CacheStore,
  HorseRaceResultsFilters,
  KvStore,
  RaceFeatureFilters,
  RaceTrainingFilters,
  SourceScope,
  WinRateHeatmapStatsFilters,
} from "./types";

export type CacheDescriptor =
  | { date: string; kind: "race-keys" }
  | {
      date: string;
      keibajoCode: string;
      kind: "race-trainings";
      raceBango: string;
    }
  | {
      date: string;
      keibajoCode?: string;
      kind: "race-features";
      raceBango?: string;
      source: SourceScope;
    }
  | {
      date: string;
      includeDistance: boolean;
      includeJockeyFrame?: boolean;
      includeOwner?: boolean;
      includeSurface: boolean;
      includeTurn: boolean;
      includeVenue: boolean;
      keibajoCode: string;
      kind: "win-rate-heatmap-stats";
      raceBango: string;
      source: "jra" | "nar";
      years: number;
    }
  | {
      date: string;
      keibajoCode: string;
      kind: "horse-race-results";
      raceBango: string;
      source: "jra" | "nar";
      sourceScope: "all" | "jra" | "nar";
    }
  | {
      date: string;
      includeDistance: boolean;
      includeSurface: boolean;
      includeTurn: boolean;
      includeVenue: boolean;
      keibajoCode: string;
      kind: "condition-history-stats";
      raceBango: string;
      source: "jra" | "nar";
      years: number;
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
  if (descriptor.kind === "race-trainings") {
    url.searchParams.set("keibajoCode", descriptor.keibajoCode);
    url.searchParams.set("raceBango", descriptor.raceBango);
  }
  if (descriptor.kind === "win-rate-heatmap-stats") {
    url.searchParams.set("keibajoCode", descriptor.keibajoCode);
    url.searchParams.set("raceBango", descriptor.raceBango);
    url.searchParams.set("source", descriptor.source);
    url.searchParams.set("years", String(descriptor.years));
    url.searchParams.set("includeVenue", descriptor.includeVenue ? "1" : "0");
    url.searchParams.set("includeDistance", descriptor.includeDistance ? "1" : "0");
    url.searchParams.set("includeSurface", descriptor.includeSurface ? "1" : "0");
    url.searchParams.set("includeTurn", descriptor.includeTurn ? "1" : "0");
    url.searchParams.set("nameTrim", "ideographic");
    url.searchParams.set("emptyTurnBypass", "1");
    if (descriptor.includeOwner === true) url.searchParams.set("includeOwner", "1");
    if (descriptor.includeJockeyFrame === true) url.searchParams.set("includeJockeyFrame", "1");
  }
  if (descriptor.kind === "horse-race-results") {
    url.searchParams.set("keibajoCode", descriptor.keibajoCode);
    url.searchParams.set("raceBango", descriptor.raceBango);
    url.searchParams.set("source", descriptor.source);
    url.searchParams.set("sourceScope", descriptor.sourceScope);
  }
  if (descriptor.kind === "condition-history-stats") {
    url.searchParams.set("keibajoCode", descriptor.keibajoCode);
    url.searchParams.set("raceBango", descriptor.raceBango);
    url.searchParams.set("source", descriptor.source);
    url.searchParams.set("years", String(descriptor.years));
    url.searchParams.set("includeVenue", descriptor.includeVenue ? "1" : "0");
    url.searchParams.set("includeDistance", descriptor.includeDistance ? "1" : "0");
    url.searchParams.set("includeSurface", descriptor.includeSurface ? "1" : "0");
    url.searchParams.set("includeTurn", descriptor.includeTurn ? "1" : "0");
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

export const readKvHeatmapStats = async (kv: KvStore, key: string): Promise<string | null> => {
  const value = await kv.get(key);
  if (value === null) return null;
  try {
    const parsed: unknown = JSON.parse(value);
    return isRecord(parsed) &&
      Array.isArray(parsed.bloodlineRows) &&
      Array.isArray(parsed.similarRows)
      ? value
      : null;
  } catch {
    return null;
  }
};

export const readKvConditionHistoryStats = async (
  kv: KvStore,
  key: string,
): Promise<string | null> => {
  const value = await kv.get(key);
  if (value === null) return null;
  try {
    const parsed: unknown = JSON.parse(value);
    return isRecord(parsed) &&
      Array.isArray(parsed.frameStats) &&
      Array.isArray(parsed.weightClassStats) &&
      Array.isArray(parsed.carriedWeightClassStats) &&
      Array.isArray(parsed.finishPositionStats) &&
      isRecord(parsed.raceTimeStats)
      ? value
      : null;
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

export const trainingDescriptor = (filters: RaceTrainingFilters): CacheDescriptor => ({
  date: filters.date,
  keibajoCode: filters.keibajoCode,
  kind: "race-trainings",
  raceBango: filters.raceBango,
});

export const heatmapStatsDescriptor = (filters: WinRateHeatmapStatsFilters): CacheDescriptor => ({
  date: filters.date,
  includeDistance: filters.includeDistance,
  includeJockeyFrame: filters.includeJockeyFrame === true,
  includeOwner: filters.includeOwner === true,
  includeSurface: filters.includeSurface,
  includeTurn: filters.includeTurn,
  includeVenue: filters.includeVenue,
  keibajoCode: filters.keibajoCode,
  kind: "win-rate-heatmap-stats",
  raceBango: filters.raceBango,
  source: filters.source,
  years: filters.years,
});

export const horseRaceResultsDescriptor = (filters: HorseRaceResultsFilters): CacheDescriptor => ({
  date: filters.date,
  keibajoCode: filters.keibajoCode,
  kind: "horse-race-results",
  raceBango: filters.raceBango,
  source: filters.source,
  sourceScope: filters.sourceScope,
});

export const conditionHistoryStatsDescriptor = (
  filters: WinRateHeatmapStatsFilters,
): CacheDescriptor => ({
  date: filters.date,
  includeDistance: filters.includeDistance,
  includeSurface: filters.includeSurface,
  includeTurn: filters.includeTurn,
  includeVenue: filters.includeVenue,
  keibajoCode: filters.keibajoCode,
  kind: "condition-history-stats",
  raceBango: filters.raceBango,
  source: filters.source,
  years: filters.years,
});
