// Run with bun. KV/Cache-API backed shared cache for `getRaceTimeStats`
// output so that the `condition` and `time-score` detail sections can
// reuse a single compute result instead of independently re-running the
// same DB query for the same race + settings combination.

import "server-only";
import { cache } from "react";

import { getRaceTimeStats } from "../db/queries";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import type { RaceSource } from "./codes";
import { DETAIL_SECTION_CACHE_AFTER_START_SECONDS } from "./race-detail-section-cache";
import type { RaceDetail, RaceTimeStats, SimilarRaceStatsSettings } from "./race-types";

const CACHE_NAMESPACE = "pc-keiba-viewer:race-time-stats:v1";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/race-time-stats-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";
const SETTINGS_HASH_HEX_LENGTH = 16;
const HEX_BASE = 16;
const HEX_PAD_LENGTH = 2;
const RACE_DAY_MONTH_START = 0;
const RACE_DAY_MONTH_END = 2;
const RACE_DAY_DAY_START = 2;
const RACE_DAY_DAY_END = 4;
const MAX_CACHE_API_MAX_AGE_SECONDS = 60;

export interface RaceTimeStatsCacheKeyParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  settingsHash: string;
  source: RaceSource;
  year: string;
}

interface PutRaceTimeStatsCacheParams {
  body: string;
  cacheKey: string;
  race: RaceDetail;
}

interface GetOrComputeRaceTimeStatsParams {
  race: RaceDetail;
  settings: SimilarRaceStatsSettings;
}

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getRaceStartTimeMs = (race: RaceDetail): number | null => {
  const normalizedTime = race.hassoJikoku?.trim().padStart(4, "0");
  if (!normalizedTime || !/^\d{4}$/u.test(normalizedTime)) {
    return null;
  }
  const startTime = Date.parse(
    `${race.kaisaiNen}-${race.kaisaiTsukihi.slice(
      RACE_DAY_MONTH_START,
      RACE_DAY_MONTH_END,
    )}-${race.kaisaiTsukihi.slice(
      RACE_DAY_DAY_START,
      RACE_DAY_DAY_END,
    )}T${normalizedTime.slice(0, 2)}:${normalizedTime.slice(2, 4)}:00+09:00`,
  );
  return Number.isFinite(startTime) ? startTime : null;
};

const getRaceDayFallbackBaseTimeMs = (race: RaceDetail): number =>
  Date.parse(
    `${race.kaisaiNen}-${race.kaisaiTsukihi.slice(
      RACE_DAY_MONTH_START,
      RACE_DAY_MONTH_END,
    )}-${race.kaisaiTsukihi.slice(RACE_DAY_DAY_START, RACE_DAY_DAY_END)}T23:59:59+09:00`,
  );

export const getRaceTimeStatsCacheTtlSeconds = (
  race: RaceDetail,
  env: CloudflareEnv | null,
  nowMs = Date.now(),
): number => {
  const parsed = Number(env?.PC_KEIBA_DETAIL_SECTION_CACHE_AFTER_START_SECONDS);
  const afterStartSeconds =
    Number.isFinite(parsed) && parsed >= MAX_CACHE_API_MAX_AGE_SECONDS
      ? Math.floor(parsed)
      : DETAIL_SECTION_CACHE_AFTER_START_SECONDS;
  const raceStartTime = getRaceStartTimeMs(race);
  const expiresAt =
    (raceStartTime ?? getRaceDayFallbackBaseTimeMs(race)) + afterStartSeconds * 1000;
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

export const buildRaceTimeStatsCacheKey = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  settingsHash,
  source,
  year,
}: RaceTimeStatsCacheKeyParams): string =>
  [CACHE_NAMESPACE, source, year, month, day, keibajoCode, raceNumber, settingsHash].join(":");

const getCacheRequest = (cacheKey: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isRaceTimeStatsPayload = (value: unknown): value is RaceTimeStats => {
  if (!isRecord(value)) return false;
  return (
    typeof value.raceCount === "number" &&
    Array.isArray(value.correlationRows) &&
    Array.isArray(value.targetRaces)
  );
};

const readPayloadFromResponse = async (response: Response): Promise<RaceTimeStats | null> => {
  try {
    const parsed: unknown = await response.json();
    return isRaceTimeStatsPayload(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

const parsePayloadFromText = (text: string): RaceTimeStats | null => {
  try {
    const parsed: unknown = JSON.parse(text);
    return isRaceTimeStatsPayload(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

export const getCachedRaceTimeStats = async (cacheKey: string): Promise<RaceTimeStats | null> => {
  const defaultCache = getDefaultCache();
  const cacheRequest = getCacheRequest(cacheKey);
  const cachedResponse = await defaultCache?.match(cacheRequest);
  if (cachedResponse?.ok) {
    const fromResponse = await readPayloadFromResponse(cachedResponse);
    if (fromResponse !== null) return fromResponse;
    await defaultCache?.delete(cacheRequest);
  }

  const { env, ctx } = await safeGetCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) {
    return null;
  }

  const parsed = parsePayloadFromText(kvBody);
  if (parsed === null) {
    return null;
  }
  const putCache = async (): Promise<void> => {
    await defaultCache?.put(
      cacheRequest,
      new Response(kvBody, {
        headers: {
          "Cache-Control": `public, max-age=${MAX_CACHE_API_MAX_AGE_SECONDS}`,
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    );
  };
  ctx?.waitUntil(putCache());
  return parsed;
};

export const putRaceTimeStatsCache = async ({
  body,
  cacheKey,
  race,
}: PutRaceTimeStatsCacheParams): Promise<void> => {
  const { env, ctx } = await safeGetCloudflareRuntime();
  const ttlSeconds = getRaceTimeStatsCacheTtlSeconds(race, env);
  if (ttlSeconds <= 0) {
    return;
  }
  const defaultCache = getDefaultCache();
  const cacheRequest = getCacheRequest(cacheKey);
  const putCaches = Promise.all([
    defaultCache?.put(
      cacheRequest,
      new Response(body, {
        headers: {
          "Cache-Control": `public, max-age=${Math.min(ttlSeconds, MAX_CACHE_API_MAX_AGE_SECONDS)}`,
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: ttlSeconds }),
  ]);
  if (ctx !== null) {
    ctx.waitUntil(putCaches);
    return;
  }
  await putCaches;
};

const byteToHex = (byte: number): string => byte.toString(HEX_BASE).padStart(HEX_PAD_LENGTH, "0");

const bufferToHex = (buffer: ArrayBuffer): string =>
  Array.from(new Uint8Array(buffer)).map(byteToHex).join("");

const compareSettingsEntryKeys = (left: [string, unknown], right: [string, unknown]): number =>
  left[0].localeCompare(right[0]);

const stableStringifySettings = (settings: SimilarRaceStatsSettings): string =>
  JSON.stringify(Object.entries(settings).toSorted(compareSettingsEntryKeys));

export const hashSettings = async (settings: SimilarRaceStatsSettings): Promise<string> => {
  const serialized = stableStringifySettings(settings);
  const encoder = new TextEncoder();
  const digest = await crypto.subtle.digest("SHA-1", encoder.encode(serialized));
  return bufferToHex(digest).slice(0, SETTINGS_HASH_HEX_LENGTH);
};

const buildCacheKeyForRace = async (params: GetOrComputeRaceTimeStatsParams): Promise<string> => {
  const settingsHash = await hashSettings(params.settings);
  return buildRaceTimeStatsCacheKey({
    day: params.race.kaisaiTsukihi.slice(RACE_DAY_DAY_START, RACE_DAY_DAY_END),
    keibajoCode: params.race.keibajoCode,
    month: params.race.kaisaiTsukihi.slice(RACE_DAY_MONTH_START, RACE_DAY_MONTH_END),
    raceNumber: params.race.raceBango,
    settingsHash,
    source: params.race.source,
    year: params.race.kaisaiNen,
  });
};

export const getOrComputeRaceTimeStats = cache(
  async (params: GetOrComputeRaceTimeStatsParams): Promise<RaceTimeStats> => {
    const cacheKey = await buildCacheKeyForRace(params);
    const cached = await getCachedRaceTimeStats(cacheKey);
    if (cached) return cached;
    const computed = await getRaceTimeStats(params.race, params.settings);
    await putRaceTimeStatsCache({
      body: JSON.stringify(computed),
      cacheKey,
      race: params.race,
    }).catch(() => undefined);
    return computed;
  },
);
