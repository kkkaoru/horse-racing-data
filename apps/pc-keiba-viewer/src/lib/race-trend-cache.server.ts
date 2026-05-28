import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import type { RaceSource } from "./codes";
import {
  RACE_TREND_CACHE_AFTER_START_SECONDS,
  addDaysToYmd,
  buildRaceTrendCacheKey,
  getRaceTrendCacheTtlSeconds,
  type RaceTrendCacheOptions,
} from "./race-trend-cache";
import type { RaceDetail } from "./race-types";
import { bustRealtimeRowsForDay } from "./realtime-trend-day-cache.server";

export interface RaceTrendBustRaceRef {
  keibajoCode: string;
  raceBango: string;
}

export interface BustRaceTrendCachesParams {
  races: ReadonlyArray<RaceTrendBustRaceRef>;
  source: RaceSource;
  targetYmd: string;
}

const CACHE_CONTROL_HEADER = "public, max-age=%d";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/race-trend-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";

type CacheSource = "cache-api" | "kv";

const memoryCache = new Map<string, { body: string; expiresAt: number }>();

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

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getCacheRequest = (cacheKey: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const getConfiguredAfterStartSeconds = (env: CloudflareEnv | null): number => {
  const parsed = Number(env?.PC_KEIBA_RACE_TREND_CACHE_AFTER_START_SECONDS);
  return Number.isFinite(parsed) && parsed >= 60
    ? Math.floor(parsed)
    : RACE_TREND_CACHE_AFTER_START_SECONDS;
};

const buildCachedResponse = (body: string, source: CacheSource | "memory"): Response =>
  new Response(body, {
    headers: {
      "Cache-Control": "public, max-age=60",
      "Content-Type": DEFAULT_CONTENT_TYPE,
      "X-Race-Trend-Cache": `HIT-${source}`,
    },
  });

export const buildRaceTrendCacheKeyForRequest = ({
  keibajoCode,
  options,
  raceNumber,
}: {
  day: string;
  keibajoCode: string;
  month: string;
  options: RaceTrendCacheOptions;
  raceNumber: string;
  year: string;
}): string => buildRaceTrendCacheKey({ keibajoCode, options, raceBango: raceNumber });

export const getCachedRaceTrendResponse = async (cacheKey: string): Promise<Response | null> => {
  const cachedMemory = memoryCache.get(cacheKey);
  if (cachedMemory) {
    if (cachedMemory.expiresAt > Date.now()) {
      return buildCachedResponse(cachedMemory.body, "memory");
    }
    memoryCache.delete(cacheKey);
  }

  const defaultCache = getDefaultCache();
  const cacheRequest = getCacheRequest(cacheKey);
  const cachedResponse = await defaultCache?.match(cacheRequest);
  if (cachedResponse?.ok) {
    return buildCachedResponse(await cachedResponse.text(), "cache-api");
  }

  const { env, ctx } = await getCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) {
    return null;
  }

  const putCache = async () => {
    await defaultCache?.put(
      cacheRequest,
      new Response(kvBody, {
        headers: {
          "Cache-Control": "public, max-age=60",
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    );
  };
  ctx?.waitUntil(putCache());
  return buildCachedResponse(kvBody, "kv");
};

export const putRaceTrendCache = async ({
  body,
  cacheKey,
  race,
}: {
  body: string;
  cacheKey: string;
  race: RaceDetail;
}): Promise<void> => {
  const { env } = await getCloudflareRuntime();
  const ttlSeconds = getRaceTrendCacheTtlSeconds(race, getConfiguredAfterStartSeconds(env));
  if (ttlSeconds <= 0) {
    return;
  }
  memoryCache.set(cacheKey, {
    body,
    expiresAt: Date.now() + ttlSeconds * 1000,
  });

  const cacheControl = CACHE_CONTROL_HEADER.replace("%d", String(ttlSeconds));
  await Promise.all([
    getDefaultCache()?.put(
      getCacheRequest(cacheKey),
      new Response(body, {
        headers: {
          "Cache-Control": cacheControl,
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: ttlSeconds }),
  ]);
};

const buildAffectedCacheOptions = (
  source: RaceSource,
  targetYmd: string,
): RaceTrendCacheOptions[] => {
  const ranges = source === "jra" ? [-1] : [-3];
  return ranges.flatMap((days) => {
    const startYmd = addDaysToYmd(targetYmd, days);
    return [true, false].map((includeRealtimeResults) => ({
      frameEndYmd: targetYmd,
      frameStartYmd: startYmd,
      includeRealtimeResults,
      jockeyEndYmd: targetYmd,
      jockeyStartYmd: startYmd,
      source,
    }));
  });
};

const D1_DAILY_CACHE_PREFIX = "race-trend-d1-daily:v1";
const D1_SNAPSHOT_CACHE_PREFIX = "race-trend-d1:v1";
const D1_DAILY_CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-daily-cache/";
const D1_SNAPSHOT_CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-cache/";

const buildAffectedD1RangeKeys = (
  source: RaceSource,
  targetYmd: string,
): Array<{ endYmd: string; startYmd: string }> => {
  const ranges = source === "jra" ? [-1] : [-3];
  return ranges.map((days) => ({
    endYmd: targetYmd,
    startYmd: addDaysToYmd(targetYmd, days),
  }));
};

interface AffectedCacheKey {
  key: string;
  urlBase: string;
}

const collectAffectedCacheKeys = (params: BustRaceTrendCachesParams): AffectedCacheKey[] => {
  const optionVariants = buildAffectedCacheOptions(params.source, params.targetYmd);
  const trendKeys = params.races.flatMap((race) =>
    optionVariants.map((options) => ({
      key: buildRaceTrendCacheKey({
        keibajoCode: race.keibajoCode,
        options,
        raceBango: race.raceBango,
      }),
      urlBase: CACHE_URL_BASE,
    })),
  );
  const d1Keys = buildAffectedD1RangeKeys(params.source, params.targetYmd).flatMap((range) => [
    {
      key: `${D1_DAILY_CACHE_PREFIX}:${params.source}:${range.startYmd}:${range.endYmd}`,
      urlBase: D1_DAILY_CACHE_URL_BASE,
    },
    {
      key: `${D1_SNAPSHOT_CACHE_PREFIX}:${params.source}:${range.startYmd}:${range.endYmd}`,
      urlBase: D1_SNAPSHOT_CACHE_URL_BASE,
    },
  ]);
  return [...trendKeys, ...d1Keys];
};

const deleteSingleCache = async (
  entry: AffectedCacheKey,
  defaultCache: Cache | null,
  env: CloudflareEnv | null,
): Promise<void> => {
  memoryCache.delete(entry.key);
  await Promise.all([
    defaultCache?.delete(new Request(`${entry.urlBase}${encodeURIComponent(entry.key)}`)),
    env?.DETAIL_SECTION_CACHE_KV?.delete(entry.key),
  ]);
};

const enumerateRealtimeDayYmds = (source: RaceSource, targetYmd: string): string[] => {
  const days = source === "jra" ? [-1, 0] : [-1, 0];
  return days.map((offset) => addDaysToYmd(targetYmd, offset));
};

export const bustRaceTrendCachesForDay = async (
  params: BustRaceTrendCachesParams,
): Promise<{ keys: string[] }> => {
  const entries = collectAffectedCacheKeys(params);
  const defaultCache = getDefaultCache();
  const { env } = await getCloudflareRuntime();
  await Promise.all([
    ...entries.map((entry) => deleteSingleCache(entry, defaultCache, env)),
    ...enumerateRealtimeDayYmds(params.source, params.targetYmd).map((ymd) =>
      bustRealtimeRowsForDay({ source: params.source, ymd }),
    ),
  ]);
  return { keys: entries.map((entry) => entry.key) };
};
