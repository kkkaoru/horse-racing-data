// Run with bun. Cache API writer for race running-style predictions.

import type { RaceRunningStyleRow, RunningStyleInferenceRace } from "./running-style-d1";
import type { Env } from "./types";

const DEFAULT_CACHE_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const CACHE_VERSION = "v3";
const CONTENT_TYPE = "application/json; charset=utf-8";

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getCacheOrigin = (env: Env): string => {
  const configured = env.RUNNING_STYLE_CACHE_ORIGIN?.trim();
  return configured && configured.length > 0 ? configured : DEFAULT_CACHE_ORIGIN;
};

const getRaceDayExpiresAtMs = (kaisaiNen: string, kaisaiTsukihi: string): number =>
  Date.parse(
    `${kaisaiNen}-${kaisaiTsukihi.slice(0, 2)}-${kaisaiTsukihi.slice(2, 4)}T23:59:59+09:00`,
  );

export const getRunningStyleCacheTtlSeconds = (
  race: Pick<RunningStyleInferenceRace, "kaisaiNen" | "kaisaiTsukihi">,
  nowMs = Date.now(),
): number => {
  const expiresAt = getRaceDayExpiresAtMs(race.kaisaiNen, race.kaisaiTsukihi);
  if (!Number.isFinite(expiresAt)) return 0;
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

export const buildRunningStyleCacheRequest = (
  env: Env,
  race: RunningStyleInferenceRace,
): Request => {
  const url = new URL(
    `/api/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(
      0,
      2,
    )}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}/running-styles`,
    getCacheOrigin(env),
  );
  url.searchParams.set("source", race.source);
  url.searchParams.set("__runningStyleCache", CACHE_VERSION);
  return new Request(url.toString());
};

const toApiRow = (row: RaceRunningStyleRow) => ({
  bamei: row.bamei,
  category: row.category,
  horseNumber: row.horseNumber,
  kaisaiNen: row.kaisaiNen,
  kettoTorokuBango: row.kettoTorokuBango,
  modelVersion: row.modelVersion,
  p_nige: row.pNige,
  p_oikomi: row.pOikomi,
  p_sashi: row.pSashi,
  p_senkou: row.pSenkou,
  predictedAt: row.predictedAt,
  predictedLabel: row.predictedLabel,
  raceKey: row.raceKey,
});

export const putRunningStyleCache = async ({
  env,
  race,
  rows,
}: {
  env: Env;
  race: RunningStyleInferenceRace;
  rows: ReadonlyArray<RaceRunningStyleRow>;
}): Promise<boolean> => {
  if (rows.length === 0) return false;
  const cache = getDefaultCache();
  if (cache === null) return false;
  const ttlSeconds = getRunningStyleCacheTtlSeconds(race);
  if (ttlSeconds <= 0) return false;
  await cache.put(
    buildRunningStyleCacheRequest(env, race),
    new Response(JSON.stringify(rows.map(toApiRow)), {
      headers: {
        "Cache-Control": `public, max-age=${ttlSeconds}`,
        "Content-Type": CONTENT_TYPE,
      },
    }),
  );
  return true;
};
