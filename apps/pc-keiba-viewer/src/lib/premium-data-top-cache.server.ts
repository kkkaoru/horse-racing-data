import "server-only";

import { getCloudflareContext } from "@opennextjs/cloudflare";

import type { RaceSource } from "./codes";
import type { PremiumDataTopHorse } from "./race-types";

const DEFAULT_CACHE_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const CACHE_VERSION = "v1";
const CONTENT_TYPE = "application/json; charset=utf-8";

export interface PremiumDataTopCacheRace {
  source: RaceSource;
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

const getRealtimeApiBaseUrl = (): string =>
  process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";

const normalizeKeibajoCode = (value: string): string => value.padStart(2, "0");
const normalizeRaceBango = (value: string): string => value.padStart(2, "0");

export const buildPremiumDataTopCacheRequest = (
  race: PremiumDataTopCacheRace,
  env: CloudflareEnv | null,
): Request => {
  const url = new URL(
    `/api/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(
      0,
      2,
    )}/${race.kaisaiTsukihi.slice(2, 4)}/${normalizeKeibajoCode(
      race.keibajoCode,
    )}/${normalizeRaceBango(race.raceBango)}/sections/premium-data-top`,
    getCacheOrigin(env),
  );
  url.searchParams.set("source", race.source);
  url.searchParams.set("__premiumDataTopCache", CACHE_VERSION);
  return new Request(url.toString());
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isPremiumDataTopHorse = (value: unknown): value is PremiumDataTopHorse => {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.horseNumber === "string" &&
    typeof value.fetchedAt === "string" &&
    typeof value.rank === "number" &&
    Array.isArray(value.reasons) &&
    value.reasons.every((reason) => typeof reason === "string")
  );
};

const parseCachedPayload = (payload: unknown): PremiumDataTopHorse[] | null => {
  if (!isRecord(payload) || !Array.isArray(payload.dataTopHorses)) {
    return null;
  }
  const rows = payload.dataTopHorses.filter(isPremiumDataTopHorse);
  return rows.length > 0 ? rows : null;
};

const readCachedRows = async (response: Response): Promise<PremiumDataTopHorse[] | null> => {
  try {
    const payload: unknown = await response.json();
    return parseCachedPayload(payload);
  } catch {
    return null;
  }
};

const fetchPremiumDataTopFromRealtimeApi = async (
  race: PremiumDataTopCacheRace,
): Promise<PremiumDataTopHorse[]> => {
  const baseUrl = getRealtimeApiBaseUrl().replace(/\/$/u, "");
  const url = `${baseUrl}/api/${race.source}/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(
    0,
    2,
  )}/${race.kaisaiTsukihi.slice(2, 4)}/${normalizeKeibajoCode(race.keibajoCode)}/${normalizeRaceBango(
    race.raceBango,
  )}/premium`;
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      return [];
    }
    const payload: unknown = await response.json();
    return parseCachedPayload(payload) ?? [];
  } catch {
    return [];
  }
};

export const getPremiumDataTopHorsesWithCache = async (
  race: PremiumDataTopCacheRace,
): Promise<PremiumDataTopHorse[]> => {
  const { env } = await getCloudflareRuntime();
  const cache = getDefaultCache();
  const cacheRequest = buildPremiumDataTopCacheRequest(race, env);
  const cachedResponse = await cache?.match(cacheRequest);
  if (cachedResponse?.ok) {
    const cachedRows = await readCachedRows(cachedResponse);
    if (cachedRows !== null) {
      return cachedRows;
    }
  }
  return fetchPremiumDataTopFromRealtimeApi(race);
};

export const putPremiumDataTopSectionCache = async ({
  env,
  race,
  payload,
}: {
  env: CloudflareEnv | null;
  race: PremiumDataTopCacheRace;
  payload: { dataTopHorses: PremiumDataTopHorse[]; type: "premium-data-top" };
}): Promise<void> => {
  if (payload.dataTopHorses.length === 0) {
    return;
  }
  const cache = getDefaultCache();
  if (cache === null) {
    return;
  }
  const expiresAt = Date.parse(
    `${race.kaisaiNen}-${race.kaisaiTsukihi.slice(0, 2)}-${race.kaisaiTsukihi.slice(
      2,
      4,
    )}T23:59:59+09:00`,
  );
  const ttlSeconds = Number.isFinite(expiresAt)
    ? Math.max(0, Math.floor((expiresAt - Date.now()) / 1000))
    : 0;
  if (ttlSeconds <= 0) {
    return;
  }
  await cache.put(
    buildPremiumDataTopCacheRequest(race, env),
    new Response(JSON.stringify(payload), {
      headers: {
        "Cache-Control": `public, max-age=${ttlSeconds}`,
        "Content-Type": CONTENT_TYPE,
      },
    }),
  );
};
