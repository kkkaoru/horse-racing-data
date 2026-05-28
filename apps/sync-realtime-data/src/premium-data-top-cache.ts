import type { PremiumDataTopHorse } from "./premium-race";
import type { NarRaceSource } from "./types";

const DEFAULT_CACHE_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const CACHE_VERSION = "v1";
const CONTENT_TYPE = "application/json; charset=utf-8";

type EnvLike = {
  PREMIUM_DATA_TOP_CACHE_ORIGIN?: string;
  RUNNING_STYLE_CACHE_ORIGIN?: string;
};

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getCacheOrigin = (env: EnvLike): string => {
  const configured =
    env.PREMIUM_DATA_TOP_CACHE_ORIGIN?.trim() || env.RUNNING_STYLE_CACHE_ORIGIN?.trim();
  return configured && configured.length > 0 ? configured : DEFAULT_CACHE_ORIGIN;
};

const getRaceDayExpiresAtMs = (kaisaiNen: string, kaisaiTsukihi: string): number =>
  Date.parse(
    `${kaisaiNen}-${kaisaiTsukihi.slice(0, 2)}-${kaisaiTsukihi.slice(2, 4)}T23:59:59+09:00`,
  );

export const getPremiumDataTopCacheTtlSeconds = (
  race: Pick<NarRaceSource, "kaisaiNen" | "kaisaiTsukihi">,
  nowMs = Date.now(),
): number => {
  const expiresAt = getRaceDayExpiresAtMs(race.kaisaiNen, race.kaisaiTsukihi);
  if (!Number.isFinite(expiresAt)) {
    return 0;
  }
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

export const buildPremiumDataTopCacheRequest = (
  env: EnvLike,
  race: Pick<NarRaceSource, "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango" | "source">,
): Request => {
  const url = new URL(
    `/api/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(
      0,
      2,
    )}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}/sections/premium-data-top`,
    getCacheOrigin(env),
  );
  url.searchParams.set("source", race.source);
  url.searchParams.set("__premiumDataTopCache", CACHE_VERSION);
  return new Request(url.toString());
};

export const putPremiumDataTopCache = async ({
  env,
  race,
  rows,
}: {
  env: EnvLike;
  race: Pick<NarRaceSource, "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "raceBango" | "source">;
  rows: ReadonlyArray<PremiumDataTopHorse & { fetchedAt: string }>;
}): Promise<boolean> => {
  if (rows.length === 0) {
    return false;
  }
  const cache = getDefaultCache();
  if (cache === null) {
    return false;
  }
  const ttlSeconds = getPremiumDataTopCacheTtlSeconds(race);
  if (ttlSeconds <= 0) {
    return false;
  }
  await cache.put(
    buildPremiumDataTopCacheRequest(env, race),
    new Response(JSON.stringify({ dataTopHorses: rows, type: "premium-data-top" }), {
      headers: {
        "Cache-Control": `public, max-age=${ttlSeconds}`,
        "Content-Type": CONTENT_TYPE,
      },
    }),
  );
  return true;
};
