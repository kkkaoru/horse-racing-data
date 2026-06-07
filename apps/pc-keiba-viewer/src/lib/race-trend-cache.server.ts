import "server-only";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import type { RaceSource } from "./codes";
import {
  RACE_TREND_CACHE_AFTER_START_SECONDS,
  RACE_TREND_PAST14_LOOKBACK_DAYS,
  addDaysToYmd,
  buildRaceTrendCacheKey,
  buildRaceTrendPast14CacheKey,
  buildRaceTrendTodayCacheKey,
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

// In-flight gate for KV PUT. When concurrent rebuilds finish at the same
// time (race-detail notify-storm) we previously sent N identical PUTs to
// DETAIL_SECTION_CACHE_KV, which 429-throttled the namespace. The gate is
// stored in KV itself so it spans Worker isolates. 60s is the KV
// expirationTtl minimum; a stale gate self-clears within that window.
const KV_PUT_IN_FLIGHT_PREFIX = "race-trend-kv-put-in-flight:";
const KV_PUT_IN_FLIGHT_TTL_SECONDS = 60;
const KV_PUT_IN_FLIGHT_VALUE = "1";

type CacheSource = "cache-api" | "kv";

const memoryCache = new Map<string, { body: string; expiresAt: number }>();

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

  const { env, ctx } = await safeGetCloudflareRuntime();
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

const getKvPutInFlightKey = (cacheKey: string): string => `${KV_PUT_IN_FLIGHT_PREFIX}${cacheKey}`;

const isKvPutInFlight = async (kv: PcKeibaKvNamespace, cacheKey: string): Promise<boolean> => {
  try {
    return (await kv.get(getKvPutInFlightKey(cacheKey))) === KV_PUT_IN_FLIGHT_VALUE;
  } catch {
    return false;
  }
};

const markKvPutInFlight = async (kv: PcKeibaKvNamespace, cacheKey: string): Promise<void> => {
  try {
    await kv.put(getKvPutInFlightKey(cacheKey), KV_PUT_IN_FLIGHT_VALUE, {
      expirationTtl: KV_PUT_IN_FLIGHT_TTL_SECONDS,
    });
  } catch {
    // Gate-mark failure is non-fatal; the main PUT below will still attempt.
  }
};

const putKvIfNotInFlight = async ({
  body,
  cacheKey,
  kv,
  ttlSeconds,
}: {
  body: string;
  cacheKey: string;
  kv: PcKeibaKvNamespace;
  ttlSeconds: number;
}): Promise<void> => {
  if (await isKvPutInFlight(kv, cacheKey)) {
    return;
  }
  await markKvPutInFlight(kv, cacheKey);
  await kv.put(cacheKey, body, { expirationTtl: ttlSeconds });
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
  const { env } = await safeGetCloudflareRuntime();
  const ttlSeconds = getRaceTrendCacheTtlSeconds(race, getConfiguredAfterStartSeconds(env));
  if (ttlSeconds <= 0) {
    return;
  }
  memoryCache.set(cacheKey, {
    body,
    expiresAt: Date.now() + ttlSeconds * 1000,
  });

  const cacheControl = CACHE_CONTROL_HEADER.replace("%d", String(ttlSeconds));
  const kv = env?.DETAIL_SECTION_CACHE_KV;
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
    kv ? putKvIfNotInFlight({ body, cacheKey, kv, ttlSeconds }) : undefined,
  ]);
};

const buildAffectedCacheOptions = (
  source: RaceSource,
  targetYmd: string,
): RaceTrendCacheOptions[] => {
  const startYmd = addDaysToYmd(targetYmd, -RACE_TREND_PAST14_LOOKBACK_DAYS);
  return [true, false].map((includeRealtimeResults) => ({
    frameEndYmd: targetYmd,
    frameStartYmd: startYmd,
    includeRealtimeResults,
    jockeyEndYmd: targetYmd,
    jockeyStartYmd: startYmd,
    source,
  }));
};

// Past-14 / today prefixes match the v8 cache keys built in
// `d1-trend-queries.server.ts` and `race-trend-cache.ts`. The bust path
// must target the same key version that the read path is currently
// writing — otherwise a race-finish notification clears a stale namespace
// and leaves the active v8 entries untouched.
const D1_PAST14_CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-past14-cache/";
const D1_TODAY_CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-today-cache/";

interface AffectedCacheKey {
  key: string;
  urlBase: string;
}

const buildPast14RangeForBust = (targetYmd: string): { endYmd: string; startYmd: string } => ({
  endYmd: addDaysToYmd(targetYmd, -1),
  startYmd: addDaysToYmd(targetYmd, -RACE_TREND_PAST14_LOOKBACK_DAYS),
});

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
  const past14Range = buildPast14RangeForBust(params.targetYmd);
  const past14Keys = params.races.map((race) => ({
    key: buildRaceTrendPast14CacheKey({
      endYmd: past14Range.endYmd,
      keibajoCode: race.keibajoCode,
      raceBango: race.raceBango,
      source: params.source,
      startYmd: past14Range.startYmd,
    }),
    urlBase: D1_PAST14_CACHE_URL_BASE,
  }));
  // v9 today key embeds keibajoCode so one entry per venue is busted.
  // Dedupe via Set because the caller passes one race per raceBango — the
  // per-venue today entry is shared across every raceBango on that venue.
  const todayKeibajoCodes = Array.from(new Set(params.races.map((race) => race.keibajoCode)));
  const todayKeys = todayKeibajoCodes.map((keibajoCode) => ({
    key: buildRaceTrendTodayCacheKey({
      keibajoCode,
      source: params.source,
      targetYmd: params.targetYmd,
    }),
    urlBase: D1_TODAY_CACHE_URL_BASE,
  }));
  return [...trendKeys, ...past14Keys, ...todayKeys];
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
  const { env } = await safeGetCloudflareRuntime();
  await Promise.all([
    ...entries.map((entry) => deleteSingleCache(entry, defaultCache, env)),
    ...enumerateRealtimeDayYmds(params.source, params.targetYmd).map((ymd) =>
      bustRealtimeRowsForDay({ source: params.source, ymd }),
    ),
  ]);
  return { keys: entries.map((entry) => entry.key) };
};
