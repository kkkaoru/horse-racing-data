import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import {
  buildRaceKey,
  getRaceRunningStylesFromD1,
  isRunningStyleLabel,
  numericOrNull,
  type RaceRunningStyleRow,
} from "../db/corner-running-style-queries";

const DEFAULT_CACHE_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const CACHE_VERSION = "v1";
const CONTENT_TYPE = "application/json; charset=utf-8";

export interface RunningStyleCacheRace {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

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
  return configured && configured.length > 0 ? configured : DEFAULT_CACHE_ORIGIN;
};

const normalizeKeibajoCode = (value: string): string => value.padStart(2, "0");
const normalizeRaceBango = (value: string): string => value.padStart(2, "0");

export const buildRunningStyleCacheRequest = (
  race: RunningStyleCacheRace,
  env: CloudflareEnv | null,
): Request => {
  const url = new URL(
    `/api/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(
      0,
      2,
    )}/${race.kaisaiTsukihi.slice(2, 4)}/${normalizeKeibajoCode(
      race.keibajoCode,
    )}/${normalizeRaceBango(race.raceBango)}/running-styles`,
    getCacheOrigin(env),
  );
  url.searchParams.set("source", race.source);
  url.searchParams.set("__runningStyleCache", CACHE_VERSION);
  return new Request(url.toString());
};

const getRaceDayExpiresAtMs = (race: Pick<RunningStyleCacheRace, "kaisaiNen" | "kaisaiTsukihi">) =>
  Date.parse(
    `${race.kaisaiNen}-${race.kaisaiTsukihi.slice(0, 2)}-${race.kaisaiTsukihi.slice(
      2,
      4,
    )}T23:59:59+09:00`,
  );

export const getRunningStyleCacheTtlSeconds = (
  race: Pick<RunningStyleCacheRace, "kaisaiNen" | "kaisaiTsukihi">,
  nowMs = Date.now(),
): number => {
  const expiresAt = getRaceDayExpiresAtMs(race);
  if (!Number.isFinite(expiresAt)) return 0;
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

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

const readCachedRows = async (response: Response): Promise<RaceRunningStyleRow[] | null> => {
  try {
    const payload: unknown = await response.json();
    if (!Array.isArray(payload)) return null;
    const rows = payload.map(parseCachedRunningStyleRow);
    return rows.length > 0 ? rows : null;
  } catch {
    return null;
  }
};

const putRunningStyleCache = async ({
  env,
  race,
  rows,
}: {
  env: CloudflareEnv | null;
  race: RunningStyleCacheRace;
  rows: ReadonlyArray<RaceRunningStyleRow>;
}): Promise<void> => {
  if (rows.length === 0) return;
  const cache = getDefaultCache();
  if (cache === null) return;
  const ttlSeconds = getRunningStyleCacheTtlSeconds(race);
  if (ttlSeconds <= 0) return;
  await cache.put(
    buildRunningStyleCacheRequest(race, env),
    new Response(JSON.stringify(rows), {
      headers: {
        "Cache-Control": `public, max-age=${ttlSeconds}`,
        "Content-Type": CONTENT_TYPE,
      },
    }),
  );
};

export const getRaceRunningStylesWithCache = async (
  race: RunningStyleCacheRace,
): Promise<RaceRunningStyleRow[]> => {
  const { ctx, env } = await getCloudflareRuntime();
  const cache = getDefaultCache();
  const cacheRequest = buildRunningStyleCacheRequest(race, env);
  const cachedResponse = await cache?.match(cacheRequest);
  if (cachedResponse?.ok) {
    const cachedRows = await readCachedRows(cachedResponse);
    if (cachedRows !== null) return cachedRows;
  }

  const rows = await getRaceRunningStylesFromD1(buildRaceKey(race));
  if (rows.length > 0) {
    const cachePut = putRunningStyleCache({ env, race, rows });
    if (ctx !== null) {
      ctx.waitUntil(cachePut);
    } else {
      await cachePut;
    }
  }
  return rows;
};
