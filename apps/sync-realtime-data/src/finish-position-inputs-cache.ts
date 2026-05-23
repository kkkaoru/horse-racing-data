// Run with bun. Cache API writer for finish-position feature inputs (parquet/D1 values).
// Final finish-position predictions are not cached here.

import type { FinishPositionInferenceRace } from "./finish-position-d1";
import type { Env } from "./types";

const DEFAULT_CACHE_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const CACHE_VERSION = "v1";
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

export const getFinishPositionInputsCacheTtlSeconds = (
  race: Pick<FinishPositionInferenceRace, "kaisaiNen" | "kaisaiTsukihi">,
  nowMs = Date.now(),
): number => {
  const expiresAt = getRaceDayExpiresAtMs(race.kaisaiNen, race.kaisaiTsukihi);
  if (!Number.isFinite(expiresAt)) {
    return 0;
  }
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

export const buildFinishPositionInputsCacheRequest = (
  env: Env,
  race: FinishPositionInferenceRace,
): Request => {
  const url = new URL(
    `/api/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(
      0,
      2,
    )}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}/sections/finish-prediction`,
    getCacheOrigin(env),
  );
  url.searchParams.set("source", race.source);
  url.searchParams.set("__finishPositionInputsCache", CACHE_VERSION);
  return new Request(url.toString());
};

export interface FinishPositionFeatureCachePayload {
  featuresR2Key: string;
  modelVersion: string;
  raceKey: string;
}

export const putFinishPositionInputsCache = async ({
  env,
  payload,
  race,
}: {
  env: Env;
  payload: FinishPositionFeatureCachePayload;
  race: FinishPositionInferenceRace;
}): Promise<boolean> => {
  const cache = getDefaultCache();
  if (cache === null) {
    return false;
  }
  const ttlSeconds = getFinishPositionInputsCacheTtlSeconds(race);
  if (ttlSeconds <= 0) {
    return false;
  }
  await cache.put(
    buildFinishPositionInputsCacheRequest(env, race),
    new Response(JSON.stringify(payload), {
      headers: {
        "Cache-Control": `public, max-age=${ttlSeconds}`,
        "Content-Type": CONTENT_TYPE,
      },
    }),
  );
  return true;
};
