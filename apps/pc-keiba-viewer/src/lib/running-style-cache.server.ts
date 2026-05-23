import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import { readD1QueryCache } from "../db/d1-query-cache.server";
import {
  isRunningStyleLabel,
  numericOrNull,
  type RaceRunningStyleRow,
} from "../db/corner-running-style-parsers";
import {
  buildProductionRunningStylesPath,
  buildRaceKey,
  buildRunningStyleCacheRequest,
  DEFAULT_RUNNING_STYLE_CACHE_ORIGIN,
  parseRaceDayFromRunningStyleRaceKey,
  parseRunningStyleRaceKey,
  type RunningStyleCacheRace,
} from "./running-style-cache";
import { fetchProductionApi, useProductionApiProxy } from "./production-api-proxy.server";

export {
  buildRaceKey,
  buildRunningStyleCacheRequest,
  getRunningStyleCacheTtlSeconds,
  parseRunningStyleRaceKey,
  type RunningStyleCacheRace,
} from "./running-style-cache";
export type { RaceRunningStyleRow } from "../db/corner-running-style-parsers";

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

const getCacheOrigin = (env: CloudflareEnv | null): string => {
  const configured = env?.PC_KEIBA_RUNNING_STYLE_CACHE_ORIGIN?.trim();
  return configured && configured.length > 0 ? configured : DEFAULT_RUNNING_STYLE_CACHE_ORIGIN;
};

const uniqueNonEmptyStrings = (values: ReadonlyArray<string>): string[] =>
  Array.from(new Set(values.filter((value) => value.length > 0)));

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const stringOrNull = (value: unknown): string | null => (typeof value === "string" ? value : null);

const requireString = (value: unknown, field: string): string => {
  if (typeof value === "string") return value;
  throw new Error(`cached running-style row missing ${field}`);
};

const requireNumber = (value: unknown, field: string): number => {
  const parsed = numericOrNull(value);
  if (parsed === null) throw new Error(`cached running-style row missing ${field}`);
  return parsed;
};

const parseCachedRunningStyleRow = (raw: unknown): RaceRunningStyleRow => {
  if (!isRecord(raw)) {
    throw new Error("cached running-style row is not an object");
  }
  const predictedLabel = raw.predictedLabel;
  if (typeof predictedLabel !== "string" || !isRunningStyleLabel(predictedLabel)) {
    throw new Error("cached running-style row has an invalid predictedLabel");
  }
  return {
    bamei: stringOrNull(raw.bamei),
    category: requireString(raw.category, "category"),
    horseNumber: requireNumber(raw.horseNumber, "horseNumber"),
    kaisaiNen: requireString(raw.kaisaiNen, "kaisaiNen"),
    kettoTorokuBango: requireString(raw.kettoTorokuBango, "kettoTorokuBango"),
    modelVersion: requireString(raw.modelVersion, "modelVersion"),
    p_nige: requireNumber(raw.p_nige, "p_nige"),
    p_oikomi: requireNumber(raw.p_oikomi, "p_oikomi"),
    p_sashi: requireNumber(raw.p_sashi, "p_sashi"),
    p_senkou: requireNumber(raw.p_senkou, "p_senkou"),
    predictedAt: requireString(raw.predictedAt, "predictedAt"),
    predictedLabel,
    raceKey: requireString(raw.raceKey, "raceKey"),
  };
};

const parseCachedRunningStyleRows = (payload: unknown): RaceRunningStyleRow[] | null => {
  if (!Array.isArray(payload)) {
    return null;
  }
  try {
    const rows = payload.map(parseCachedRunningStyleRow);
    return rows.length > 0 ? rows : null;
  } catch {
    return null;
  }
};

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

const readHashCachedRunningStyles = async (raceKey: string): Promise<RaceRunningStyleRow[] | null> =>
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
  const { env } = await getCloudflareRuntime();
  const raceKey = buildRaceKey(race);

  const [urlCached, hashCached] = await Promise.all([
    readUrlCachedRunningStyles(race, env),
    readHashCachedRunningStyles(raceKey),
  ]);
  const cached = pickNewerRunningStyleRows(urlCached, hashCached);
  if (cached !== null) {
    return cached;
  }

  return fetchRunningStylesFromProduction(race);
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
  const cached = await readD1QueryCache<RaceRunningStyleRow[]>(
    "horse-running-style-history",
    ["getHorseRecentRunningStylesFromD1", kettoTorokuBango, safeLimit],
  );
  if (cached !== null) {
    return cached;
  }
  return fetchHorseRunningStylesFromProduction(kettoTorokuBango, safeLimit);
};
