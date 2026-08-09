import "server-only";
import { type RaceRunningStyleRow } from "../db/corner-running-style-parsers";
import { readD1QueryCache } from "../db/d1-query-cache.server";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import {
  buildRunningStylePredictionCacheKeyFromRace,
  parseCachedRunningStyleRows,
  parsePredictionRunningStyleText,
} from "./prediction-kv-cache";
import { readPredictionKvText, writePredictionKvText } from "./prediction-kv-cache.server";
import { fetchProductionApi, useProductionApiProxy } from "./production-api-proxy.server";
import {
  buildProductionRunningStylesPath,
  buildRaceKey,
  buildRunningStyleCacheRequest,
  DEFAULT_RUNNING_STYLE_CACHE_ORIGIN,
  parseRaceDayFromRunningStyleRaceKey,
  parseRunningStyleRaceKey,
  type RunningStyleCacheRace,
} from "./running-style-cache";

export {
  buildRaceKey,
  buildRunningStyleCacheRequest,
  getRunningStyleCacheTtlSeconds,
  parseRunningStyleRaceKey,
  type RunningStyleCacheRace,
} from "./running-style-cache";
export type { RaceRunningStyleRow } from "../db/corner-running-style-parsers";

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getCacheOrigin = (env: CloudflareEnv | null): string => {
  const configured = env?.PC_KEIBA_RUNNING_STYLE_CACHE_ORIGIN?.trim();
  return configured && configured.length > 0 ? configured : DEFAULT_RUNNING_STYLE_CACHE_ORIGIN;
};

const uniqueNonEmptyStrings = (values: ReadonlyArray<string>): string[] =>
  Array.from(new Set(values.filter((value) => value.length > 0)));

const readCachedRows = async (response: Response): Promise<RaceRunningStyleRow[] | null> => {
  try {
    return parseCachedRunningStyleRows(await response.json());
  } catch {
    return null;
  }
};

const maxPredictedAtMillis = (rows: ReadonlyArray<RaceRunningStyleRow>): number =>
  rows.reduce((max, row) => {
    const parsed = Date.parse(row.predictedAt);
    return Number.isFinite(parsed) ? Math.max(max, parsed) : max;
  }, 0);

const pickNewerRunningStyleRows = (
  primary: RaceRunningStyleRow[] | null,
  secondary: RaceRunningStyleRow[] | null,
): RaceRunningStyleRow[] | null => {
  if (primary === null || primary.length === 0) {
    return secondary;
  }
  if (secondary === null || secondary.length === 0) {
    return primary;
  }
  return maxPredictedAtMillis(secondary) > maxPredictedAtMillis(primary) ? secondary : primary;
};

const readUrlCachedRunningStyles = async (
  race: RunningStyleCacheRace,
  env: CloudflareEnv | null,
): Promise<RaceRunningStyleRow[] | null> => {
  const cache = getDefaultCache();
  const cacheRequest = buildRunningStyleCacheRequest(race, getCacheOrigin(env));
  const cachedResponse = await cache?.match(cacheRequest);
  if (!cachedResponse?.ok) {
    return null;
  }
  return readCachedRows(cachedResponse);
};

const readHashCachedRunningStyles = async (
  raceKey: string,
): Promise<RaceRunningStyleRow[] | null> =>
  readD1QueryCache<RaceRunningStyleRow[]>(
    "running-style-race",
    ["getRaceRunningStylesFromD1", raceKey],
    { raceDay: parseRaceDayFromRunningStyleRaceKey(raceKey) },
  );

const fetchRunningStylesFromProduction = async (
  race: RunningStyleCacheRace,
): Promise<RaceRunningStyleRow[]> => {
  if (!useProductionApiProxy()) {
    return [];
  }
  try {
    const response = await fetchProductionApi(buildProductionRunningStylesPath(race));
    if (!response.ok) {
      return [];
    }
    return parseCachedRunningStyleRows(await response.json()) ?? [];
  } catch {
    return [];
  }
};

const fetchHorseRunningStylesFromProduction = async (
  kettoTorokuBango: string,
  limit: number,
): Promise<RaceRunningStyleRow[]> => {
  if (!useProductionApiProxy()) {
    return [];
  }
  try {
    const response = await fetchProductionApi(
      `/api/horses/${encodeURIComponent(kettoTorokuBango)}/running-styles?limit=${limit}`,
    );
    if (!response.ok) {
      return [];
    }
    return parseCachedRunningStyleRows(await response.json()) ?? [];
  } catch {
    return [];
  }
};

export const getRaceRunningStylesWithCache = async (
  race: RunningStyleCacheRace,
): Promise<RaceRunningStyleRow[]> => {
  const { env } = await safeGetCloudflareRuntime();
  const raceKey = buildRaceKey(race);

  // Prediction KV/Cache-API tier. Serves yesterday/today/tomorrow races
  // without hitting the URL cache, hash cache, or production API; on race day
  // the today TTL is short (30s) so a weight-rescore overwrite from
  // finish-position-cron/sync-realtime-data shows up at the edge quickly.
  const predictionKey = buildRunningStylePredictionCacheKeyFromRace({
    kaisaiNen: race.kaisaiNen,
    kaisaiTsukihi: race.kaisaiTsukihi,
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
  });
  const predictionWindowYmd = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
  if (predictionKey !== null) {
    const predictionBody = await readPredictionKvText(predictionKey, predictionWindowYmd);
    if (predictionBody !== null) {
      const predictionRows = parsePredictionRunningStyleText(predictionBody);
      if (predictionRows !== null) {
        return predictionRows;
      }
    }
  }

  const [urlCached, hashCached] = await Promise.all([
    readUrlCachedRunningStyles(race, env),
    readHashCachedRunningStyles(raceKey),
  ]);
  const cached = pickNewerRunningStyleRows(urlCached, hashCached);
  if (cached !== null) {
    return cached;
  }

  const productionRows = await fetchRunningStylesFromProduction(race);
  if (predictionKey !== null && productionRows.length > 0) {
    await writePredictionKvText({
      body: JSON.stringify(productionRows),
      cacheKey: predictionKey,
      raceYmd: predictionWindowYmd,
    }).catch(() => undefined);
  }
  return productionRows;
};

export const getRaceRunningStylesByRaceKeysWithCache = async (
  raceKeys: ReadonlyArray<string>,
): Promise<RaceRunningStyleRow[]> => {
  const uniqueRaceKeys = uniqueNonEmptyStrings(raceKeys);
  if (uniqueRaceKeys.length === 0) {
    return [];
  }

  const batchCached = await readD1QueryCache<RaceRunningStyleRow[]>(
    "running-style-races",
    ["getRaceRunningStylesByRaceKeysFromD1", uniqueRaceKeys],
    { raceDay: parseRaceDayFromRunningStyleRaceKey(uniqueRaceKeys[0] ?? "") },
  );
  if (batchCached !== null && batchCached.length > 0) {
    return batchCached;
  }

  const perRaceRows = await Promise.all(
    uniqueRaceKeys.map(async (raceKey) => {
      const race = parseRunningStyleRaceKey(raceKey);
      if (race === null) {
        return [];
      }
      return getRaceRunningStylesWithCache(race);
    }),
  );
  return perRaceRows.flat();
};

export const getHorseRecentRunningStylesWithCache = async (
  kettoTorokuBango: string,
  limit: number,
): Promise<RaceRunningStyleRow[]> => {
  const safeLimit = Math.max(1, Math.min(Math.trunc(limit), 100));
  const cached = await readD1QueryCache<RaceRunningStyleRow[]>("horse-running-style-history", [
    "getHorseRecentRunningStylesFromD1",
    kettoTorokuBango,
    safeLimit,
  ]);
  if (cached !== null) {
    return cached;
  }
  return fetchHorseRunningStylesFromProduction(kettoTorokuBango, safeLimit);
};
